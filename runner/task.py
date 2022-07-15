from typing import NamedTuple, Iterable, Optional

from runner.configs import get_cmd

RUN_NAME = 'run'


def set_run_name(name: str):
    global RUN_NAME
    RUN_NAME = name


def _get_task_name(config: dict) -> str:
    config_name = config.get('config_name')
    if config_name is not None:
        return RUN_NAME + ':' + config_name
    return RUN_NAME


class Task(NamedTuple):
    group_id: int
    cmd: str
    task_name: str
    memory_needed: float
    output: Optional[str]
    exit_code: Optional[int] = None


def set_exit_code(task: Task, code: int) -> Task:
    return Task(
        group_id=task.group_id,
        cmd=task.cmd,
        task_name=task.task_name,
        memory_needed=task.memory_needed,
        output=task.output,
        exit_code=code
    )


def _get_tasks(group_id: int, config_group: Iterable[dict]) -> Iterable[Task]:
    for config in config_group:
        yield Task(
            cmd=get_cmd(config),
            task_name=_get_task_name(config),
            memory_needed=config['memory_needed'],
            group_id=group_id,
            output=config.get('output')
        )
