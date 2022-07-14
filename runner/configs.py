from copy import deepcopy
from itertools import product
from typing import Iterable, Tuple, Union, List


def _collect_variable_args(args: Iterable[Union[str, List[str]]]) -> Tuple[Tuple[int, ...], Tuple[Tuple[str, ...], ...]]:
    """Collect positions and values of variable argument. Returns (positions, values)."""
    positions = []
    values = []
    for arg_idx, arg in enumerate(args):
        if isinstance(arg, list):
            positions.append(arg_idx)
            values.append(tuple(arg))

    return tuple(positions), tuple(values)


def _populate_arguments(values: Iterable[str], positions: Iterable[int], config: dict) -> None:
    if 'args' not in config:
        return
    args = config['args']
    for value, position in zip(values, positions):
        args[position] = value


def extract_configs(variable_configs: Iterable[dict]) -> Iterable[Tuple[dict, ...]]:
    """Returns configs grouped by origin"""
    for config in variable_configs:
        args = config.get('args', tuple())

        variable_positions, variable_values = _collect_variable_args(args)

        if not len(variable_values):
            yield deepcopy(config),
            continue

        new_configs = []

        for arg_variations in product(*variable_values):
            new_config = deepcopy(config)
            _populate_arguments(arg_variations, variable_positions, new_config)
            new_configs.append(new_config)

        yield tuple(new_configs)


def get_cmd(config: dict) -> str:
    """Get shell command from config without variables"""
    return ' '.join([config['command']] + config.get('args', []))
