from itertools import starmap, chain
from pprint import pprint
from typing import Tuple, List, NamedTuple, Iterable

from tqdm import tqdm

from runner.configs import get_cmd


RUN_NAME = 'run'


def set_run_name(name: str):
    global RUN_NAME
    RUN_NAME = name


def _get_run_name(config: dict) -> str:
    config_name = config.get('config_name')
    if config_name is not None:
        return RUN_NAME + ':' + config_name
    return RUN_NAME


class Task(NamedTuple):
    cmd: str
    run_name: str
    memory_needed: float


def _get_tasks(config_group: Iterable[dict]) -> Iterable[Task]:
    for config in config_group:
        yield Task(cmd=get_cmd(config), run_name=_get_run_name(config), memory_needed=config['memory_needed'])


def run_config_groups(config_groups: Iterable[Tuple[dict, ...]], progress_bars: List[tqdm]):
    tasks_with_group_idx = []
    for idx, config_group in enumerate(config_groups):
        for task in _get_tasks(config_group):
            tasks_with_group_idx.append((task, idx))

    pprint(tuple(tasks_with_group_idx))
