import asyncio
import logging
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from itertools import chain
from pathlib import Path
from time import time, sleep
from typing import Iterable, Optional, Tuple

from psutil import Process

from runner.task import Task, set_exit_code
from runner.util import to_gb, prepare_cmd

logger = logging.getLogger()

try:
    from nvitop import Device, NVMLError

    Device.count()
except NVMLError as e:
    logger.error('GPU status checking is disabled!')
    from runner.stub_nvitop import Device

loop = ThreadPoolExecutor()


class GPUWatcher:

    def __init__(self, gpu: int, *, wait_time: float = 300, utilization_limit: float = 0.9):
        self._device = Device(gpu)
        self._reserved_space = 0.0
        self._occupied_space = 0.0
        self._foreign_pids = None
        self._own_pids = set()
        self._unavailable_until = 0.0
        self._wait_time = wait_time
        self._utilization_limit = utilization_limit
        self._finished_tasks = []
        self._last_task_id = 0

        self._update()

    def submit_task(self, task: Task):
        self._update()
        self._reserved_space += task.memory_needed
        loop.submit(self._run_task, task)

    def _run_task(self, task: Task) -> None:
        logger.info(f'Task is being put on device {self._device.index}!')

        redirect = asyncio.subprocess.PIPE
        if task.output is not None:
            output_path = Path(task.output)
            os.makedirs(output_path.parent, exist_ok=True)
            redirect = open(task.output, 'a')

        child_env = os.environ.copy()
        child_env['CUDA_VISIBLE_DEVICES'] = str(self._device.index)
        child_env['TASK_NAME'] = task.task_name
        child_process = subprocess.Popen(prepare_cmd(task.cmd), stdout=redirect, stderr=redirect, env=child_env)

        child_pid = child_process.pid
        self._own_pids.add(child_pid)
        child_process.wait()
        self._own_pids.remove(child_pid)

        self._reserved_space -= task.memory_needed

        finished_task = set_exit_code(task, child_process.returncode)
        self._finished_tasks.append(finished_task)

        if task.output is not None:
            redirect.close()

    def get_finished(self) -> Tuple[Task, ...]:
        finished = tuple(self._finished_tasks)
        self._finished_tasks = []
        return finished

    @property
    def available_memory(self) -> float:
        self._update()

        # give time for foreign processes to allocate memory
        if time() < self._unavailable_until:
            logger.debug(f'[device:{self._device.index}] Rejected. Reason: waiting on foreign process.')
            return 0.0

        # do not make available if utilization limit is exceeded
        if self._device.gpu_utilization() >= self._utilization_limit:
            logger.debug(f'[device:{self._device.index}] Rejected. Reason: GPU utilization is too high.')
            return 0.0

        return to_gb(self._device.memory_total()) - self._occupied_space - self._reserved_space

    @property
    def status(self) -> str:
        self._update()
        return f'Total: {to_gb(self._device.memory_total())}\n' \
               f'Occupied: {self._occupied_space}\n' \
               f'Reserved: {self._reserved_space}\n' \
               f'Available: {self.available_memory}\n' \
               f'Running PIDs: {self._own_pids}'

    def _update(self):
        processes = self._device.processes()

        # update own pids with their children
        for pid in self._own_pids:
            children_pids = set(child.pid for child in Process(pid).children(recursive=True))
            self._own_pids.update(children_pids)

        current_foreign_pids = {pid for pid, process in processes.items() if pid not in self._own_pids}
        if self._foreign_pids is None:
            self._foreign_pids = current_foreign_pids

        if set(current_foreign_pids).difference(set(self._foreign_pids)):
            # new processes appeared
            logger.info(f'Foreign processor detected on device {self._device.index}: '
                        f'fp{self._foreign_pids}, '
                        f'cfp{current_foreign_pids}, '
                        f'op{self._own_pids}')
            self._unavailable_until = time() + self._wait_time

        self._foreign_pids = current_foreign_pids
        bytes_memory = (processes[pid].gpu_memory() for pid in self._foreign_pids)
        self._occupied_space = sum(map(to_gb, bytes_memory))


class Scheduler:

    def __init__(
            self,
            gpus: Optional[Iterable[int]] = None,
            *,
            check_interval: float = 1.0,
            concurrent_jobs: Optional[int] = None,
            wait_time: float = 300,
            utilization_limit: float = 0.9
    ):
        if gpus is None:
            gpus = range(Device.count())
        watcher_builder = partial(GPUWatcher, wait_time=wait_time, utilization_limit=utilization_limit)
        self._watchers = tuple(map(watcher_builder, gpus))
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

        self._last_check_time = time()

        # find available gpu
        free_watcher = None
        for watcher in self._watchers:
            if watcher.available_memory >= task.memory_needed:
                free_watcher = watcher
                break

        if free_watcher is None:
            logger.debug(f'No GPU is available.')
            return False

        free_watcher.submit_task(task)
        self._running_jobs += 1
        return True

    def get_finished(self) -> Tuple[Task, ...]:
        finished = tuple(chain.from_iterable(map(GPUWatcher.get_finished, self._watchers)))
        self._running_jobs -= len(finished)
        return finished
