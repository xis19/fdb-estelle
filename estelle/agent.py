import click

from .lib.config import load_config
from .lib.agent.core import agent_core


@click.group()
@click.option(
    "-c",
    "--config-path",
    default=None,
    required=False,
    help="Path to the config file",
    envvar="ESTELLE_AGENT_CONFIG_FILE",
)
def cli(config_path):
    if config_path is not None:
        load_config(config_path)


@cli.command()
def start():
    agent_core()


if __name__ == "__main__":
    cli()
