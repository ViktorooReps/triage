# TRIAGE

Intended use: running a multitude of GPU-intensive scripts in a way that optimizes GPU memory utilization. Great for ML/DL based experiments on servers shared between several users.

## Installation

```bash
pip install triage-runner
```

## Usage

See `--help` option for extended list of possible arguments.  
Running one config:
```bash
triage run_config.json
```
Running several configs:
```bash
triage run_config1.json run_config2.json run_config3.json 
```
Patterns can be used for config discovery as well:
```bash
triage run_config*.json
```
More on pattern syntax can be found here: https://docs.python.org/3.10/library/pathlib.html#pathlib.Path.glob

## Run configurations

Stored in JSON format. The sample run configuration looks like this:
```json
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
```
Every entry in `args` list is an argument for `command`. An entry can be a list - in which case TRIAGE will iterate through all the possible combinations of all values in list entries. The example script above will be run 3 times with an argument `--seed` set to 1, 2 and 3.

Parameter `config_name` is optional and is used for logging the results (see `--logfile` option TODO). Based on this parameter environment variable `TASK_NAME` is set in order to be used by running script.
