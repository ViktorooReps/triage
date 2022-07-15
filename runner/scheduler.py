import asyncio
import logging
import os
from itertools import chain
from pathlib import Path
from time import time, sleep
from typing import Iterable, Optional, Tuple

from nvitop import Device

from runner.task import Task
from runner.util import to_gb


logger = logging.getLogger()


class GPUWatcher:

    def __init__(self, gpu: int, *, wait_time: float = 300):
        self._device = Device(gpu)
        self._reserved_space = 0.0
        self._occupied_space = 0.0
        self._foreign_pids = set()
        self._unavailable_until = 0.0
        self._wait_time = wait_time
        self._running_tasks = []

        self._update()

    def submit_task(self, task: Task):
        self._reserved_space += task.memory_needed
        self._running_tasks.append(self._run_task(task))

    async def _run_task(self, task: Task) -> Task:
        redirect = asyncio.subprocess.PIPE
        if task.output is not None:
            output_path = Path(task.output)
            os.makedirs(output_path.parent, exist_ok=True)
            redirect = open(task.output, 'a')

        child_env = os.environ.copy()
        child_env['CUDA_AVAILABLE_DEVICES'] = str(self._device)
        child_env['TASK_NAME'] = task.task_name
        running_task = await asyncio.create_subprocess_shell(task.cmd, stdout=redirect, stderr=redirect)
        task.exit_code = running_task.returncode

        return task

    def get_finished(self) -> Tuple[Task, ...]:
        done, _ = await asyncio.wait(self._running_tasks, timeout=0)
        return tuple(done)

    @property
    def available_memory(self) -> float:
        self._update()
        if time() < self._unavailable_until:
            return 0.0

        return self._device.memory_total() - self._occupied_space - self._reserved_space

    def _update(self):
        processes = self._device.processes()
        current_foreign_pids = {pid for pid, process in processes.items() if process.username != os.getlogin()}
        if set(current_foreign_pids).difference(set(self._foreign_pids)):
            # new processes appeared
            self._unavailable_until = time() + self._wait_time

        bytes_memory = (processes[pid].gpu_memory() for pid in current_foreign_pids)
        self._occupied_space = sum(map(to_gb, bytes_memory))


class Scheduler:

    def __init__(self, gpus: Optional[Iterable[int]] = None, *, check_interval: float = 1.0, concurrent_jobs: Optional[int] = None):
        if gpus is None:
            gpus = range(Device.count())
        self._watchers = tuple(map(GPUWatcher, gpus))
        self._last_check_time = 0
        self._check_interval = check_interval
        self._concurrent_jobs = concurrent_jobs
        self._running_jobs = 0

    def schedule(self, task: Task) -> bool:
        # check for job limit
        if self._concurrent_jobs is not None and self._running_jobs >= self._concurrent_jobs:
            return False

        # check for time interval
        wait_for = self._check_interval - (time() - self._last_check_time)
        if wait_for > 0:
            sleep(wait_for)

        # find available gpu
        free_watcher = None
        for watcher in self._watchers:
            if watcher.available_memory >= task.memory_needed:
                free_watcher = watcher
                break

        if free_watcher is None:
            return False

        free_watcher.submit_task(task)
        self._running_jobs += 1
        return True

    def get_finished(self) -> Tuple[Task, ...]:
        finished = tuple(chain.from_iterable(map(GPUWatcher.get_finished, self._watchers)))
        self._running_jobs -= len(finished)
        return finished
