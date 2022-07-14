import click as click


@click.group(invoke_without_command=True)
@click.option('-v', '--version', is_flag=True, help='Print overseer version.')
@click.option('--debug', is_flag=True, help='Set logging level to DEBUG.')
def cli(version: bool, debug: bool):
    if version:
        print(f'Overseer {__version__}')


main = cli


if __name__ == '__main__':
    main()
