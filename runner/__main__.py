import datetime
import json
from typing import Iterable, TextIO, Optional

import click as click

import pkg_resources
from tqdm import tqdm

from runner.concurrency import run_config_groups, set_run_name
from runner.configs import extract_configs

RUN_CONFIG_HELP = """
Running several configs: $triage run_config1.json run_config2.json run_config3.json  

Patterns can be used for config discovery as well: $triage run_config*.json   

More on pattern syntax can be found here: https://docs.python.org/3.10/library/pathlib.html#pathlib.Path.glob

Run configurations are stored in JSON format. The sample run configuration looks like this:

\b
{  
  "memory_needed": 10.0,
  "config_name": "sample_config",
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
\f
"""

__version__ = pkg_resources.require('triage-runner')[0].version


@click.command(help=RUN_CONFIG_HELP)
@click.argument('run_configs', nargs=-1, type=click.File('r'))
@click.option('-n', '--run_name', type=click.STRING,
              help='Run name. Will be used for setting RUN_NAME environment variable. Datetime by default')
@click.option('-v', '--version', is_flag=True,
              help='Print overseer version.')
@click.option('-j', '--jobs', type=click.INT,
              help='Maximum number of concurrently running configs. No limit by default.', default=-1)
def cli(run_configs: Iterable[TextIO], run_name: Optional[str], version: bool, jobs: int):
    if version:
        print(f'TRIAGE {__version__}')
        return

    if run_name is None:
        run_name = datetime.datetime.now().strftime('[%d_%b_%H:%M]')
    set_run_name(run_name)

    file_names = [run_config.name for run_config in run_configs]

    def read_and_close(file: TextIO) -> dict:
        content: dict = json.load(file)
        file.close()
        return content

    variable_configs = tuple(map(read_and_close, run_configs))
    grouped_configs = tuple(extract_configs(variable_configs))

    config_names = [config.get('config_name') for config in variable_configs]

    tqdm_descriptions = [(cn if cn is not None else fn) for cn, fn in zip(config_names, file_names)]
    progress_bars = [tqdm(desc=desc, total=len(cf_group)) for desc, cf_group in zip(tqdm_descriptions, grouped_configs)]

    run_config_groups(grouped_configs, progress_bars)


main = cli


if __name__ == '__main__':
    main()
