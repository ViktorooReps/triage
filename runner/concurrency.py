import logging
from collections import Counter
from functools import partial
from itertools import chain, starmap
from typing import Tuple, List, Iterable, Callable, Sequence

from tqdm import tqdm

from runner.scheduler import Scheduler
from runner.task import _get_tasks, Task
from runner.util import apply

logger = logging.getLogger()


def run(tasks: Iterable[Task], scheduler: Scheduler, *, completion_callback: Callable[[Task], None]) -> List[Task]:
    """Schedule tasks and wait for completion. Return failed tasks."""

    tasks = tuple(tasks)

    logger.info(f'Running {len(tasks)} tasks')

    completed: List[Task] = []

    def handle_completion(completed_task: Task) -> None:
        completed.append(completed_task)

        # log info
        message_prefix = f'[{len(completed)}/{len(tasks)}] ({completed_task.memory_needed:.2f}Gb) '
        message_body = ('Task failed: ' if completed_task.exit_code else 'Task succeeded: ') + completed_task.cmd
        logger.info(message_prefix + message_body)

        completion_callback(task)

    for task_idx, task in enumerate(tasks):
        while not scheduler.schedule(task):
            completed_tasks = scheduler.get_finished()
            apply(handle_completion, completed_tasks)

        logger.info(f'[{task_idx + 1}/{len(tasks)}] ({task.memory_needed:.2f}Gb) Task scheduled to run: {task.cmd}.')

    logger.info('Scheduled all tasks!')

    while len(completed) < len(tasks):
        completed_tasks = scheduler.get_finished()
        apply(handle_completion, completed_tasks)

    return [task for task in completed if task.exit_code]


def run_config_groups(config_groups: Iterable[Tuple[dict, ...]], progress_bars: Sequence[tqdm], scheduler: Scheduler, *, repeat: int = 1):
    task_iterator = chain.from_iterable(starmap(_get_tasks, enumerate(config_groups)))
    tasks = sorted(task_iterator, key=lambda t: t.memory_needed)

    def update_progress_bars(completed_task: Task) -> None:
        progress_bars[completed_task.group_id].update()

    runner = partial(run, scheduler=scheduler, completion_callback=update_progress_bars)

    failed_tasks = runner(tasks)

    while repeat and len(failed_tasks):
        logger.info(f'Repeating failed tasks: {len(failed_tasks)}/{len(tasks)}')

        counter = Counter(task.group_id for task in failed_tasks)

        # resetting progress bars
        for group_id, progress_bar in enumerate(progress_bars):
            new_count = counter.get(group_id)
            if new_count is None:
                progress_bar.close()
            else:
                progress_bar.reset(total=new_count)

        failed_tasks = runner(failed_tasks)
        repeat -= 1

    logger.info(f'Failed tasks: {len(failed_tasks)}/{len(tasks)}')
