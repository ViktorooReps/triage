import datetime
import json
import logging
from typing import Iterable, TextIO, Optional

import click as click
import tqdm

import pkg_resources
from click_loglevel import LogLevel

from runner.concurrency import run_config_groups
from runner.task import set_run_name
from runner.configs import extract_configs
from runner.scheduler import Scheduler
from runner.tqdm_logger import setup_logging

setup_logging()
logger = logging.getLogger()

RUN_CONFIG_HELP = """
Running several configs: $ triage run_config1.json run_config2.json run_config3.json  

Patterns can be used for config discovery as well: $ triage run_config*.json   

More on pattern syntax can be found here: https://docs.python.org/3.10/library/pathlib.html#pathlib.Path.glob

Run configurations are stored in JSON format. The sample run configuration looks like this:

\b
{  
  "memory_needed": 10.0,
  "config_name": "sample_config",
  "output": "logs/sample_config_output.log"
  "command": "python3 train.py",
  "args": [
    "arg1",
    "--arg2",
    ["--seed=1", "--seed=2", "--seed=3"],
    "--arg3=3"
  ]   
}   

Every entry in `args` list is an argument for `command`. 
An entry can be a list - in which case TRIAGE will iterate 
through all the possible combinations of all values in list entries. 
The example script above will be run 3 times with an argument 
`--seed` set to 1, 2 and 3.

Parameter `config_name` is optional and is used for logging 
the results (see `--logfile` option). Based on this parameter 
environment variable `RUN_NAME` is set in order to be used by running script.

Parameter `output` is optional as well. Its value will be used as a path to
a file that will be used to redirect stdout and stdin of the running
command.
\f
"""

__version__ = pkg_resources.require('triage-runner')[0].version


def format_gpus(gpus: str) -> Iterable[int]:
    return map(int, gpus.split(','))


@click.command(help=RUN_CONFIG_HELP)
@click.argument('run_configs', nargs=-1, type=click.File('r'))
@click.option('-n', '--run_name', type=click.STRING,
              help='Run name. Will be used for setting RUN_NAME environment variable. Datetime by default')
@click.option('-v', '--version', is_flag=True,
              help='Print overseer version.')
@click.option('-j', '--jobs', type=click.INT,
              help='Maximum number of concurrently running configs. No limit by default.')
@click.option('-g', '--gpus', type=click.STRING,
              help='What GPUs to use. All available by default. Format: 0,2,3,1')
@click.option('-i', '--gpu_ping_interval', type=click.FLOAT,
              help='Interval in seconds between GPU status checks.')
@click.option("-l", "--log_level", type=LogLevel(), default=logging.NOTSET,
              help='Logging level.')
def cli(
        run_configs: Iterable[TextIO],
        run_name: Optional[str],
        version: bool,
        jobs: int,
        gpus: Optional[str],
        gpu_ping_interval: float,
        log_level: str
):
    if version:
        print(f'TRIAGE {__version__}')
        return

    if run_name is None:
        run_name = datetime.datetime.now().strftime('[%d_%b_%H:%M]')
    set_run_name(run_name)

    if gpus is not None:
        gpus = format_gpus(gpus)

    logger.setLevel(log_level)

    file_names = [run_config.name for run_config in run_configs]
    logger.info(f'Found {len(file_names)} config files.')

    def read_and_close(file: TextIO) -> dict:
        content: dict = json.load(file)
        file.close()
        return content

    variable_configs = tuple(map(read_and_close, run_configs))
    grouped_configs = tuple(extract_configs(variable_configs))

    config_names = [config.get('config_name') for config in variable_configs]

    tqdm_descriptions = [(cn if cn is not None else fn) for cn, fn in zip(config_names, file_names)]
    progress_bars = [tqdm.tqdm(desc=desc, total=len(cf_group)) for desc, cf_group in zip(tqdm_descriptions, grouped_configs)]

    run_config_groups(grouped_configs, progress_bars, Scheduler(gpus, check_interval=gpu_ping_interval, concurrent_jobs=jobs))


main = cli


if __name__ == '__main__':
    main()
