import logging
from itertools import chain, starmap
from typing import Tuple, List, Iterable

from tqdm import tqdm

from runner.scheduler import Scheduler
from runner.task import _get_tasks, Task
from runner.util import apply

logger = logging.getLogger()


def run_config_groups(config_groups: Iterable[Tuple[dict, ...]], progress_bars: List[tqdm], scheduler: Scheduler):
    task_iterator = chain.from_iterable(starmap(_get_tasks, enumerate(config_groups)))
    tasks = sorted(task_iterator, key=lambda t: t.memory_needed)

    logger.info(f'Running {len(tasks)} tasks')

    completed: List[Task] = []

    def handle_completion(completed_task: Task) -> None:
        completed.append(completed_task)

        # log info
        message_prefix = f'[{len(completed)}/{len(tasks)}] ({completed_task.memory_needed:.2f}Gb) '
        message_body = ('Task failed: ' if completed_task.exit_code else 'Task succeeded: ') + completed_task.cmd
        logger.info(message_prefix + message_body)

        # update bars
        progress_bars[completed_task.group_id].update()

    for task_idx, task in enumerate(tasks):
        while not scheduler.schedule(task):
            completed_tasks = scheduler.get_finished()
            apply(handle_completion, completed_tasks)

        logger.info(f'[{task_idx + 1}/{len(tasks)}] ({task.memory_needed:.2f}Gb) Task scheduled to run: {task.cmd}.')

    logger.info('Scheduled all tasks!')

    while len(completed) < len(tasks):
        completed_tasks = scheduler.get_finished()
        apply(handle_completion, completed_tasks)

    logger.info('Completed all tasks!')

    failed_tasks = [task for task in completed if task.exit_code]

    logger.info(f'Failed tasks: {len(failed_tasks)}/{len(tasks)}')
