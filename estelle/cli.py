import getpass
import pathlib
import sys

from typing import List, Optional

import typer

from loguru import logger
from typing_extensions import Annotated

from estelle.lib.config import config
from estelle.lib.ensemble import EnsembleState

_cli = typer.Typer()

CURRENT_USER_NAME = getpass.getuser()


@_cli.command()
def start(
    test_package: Annotated[
        pathlib.Path,
        typer.Option(help="Package contains the test context, e.g. correctness.tar.gz"),
    ],
    test_command: Annotated[
        pathlib.Path,
        typer.Option(help="Command used to trigger the test in the context"),
    ],
    user: Annotated[
        str,
        typer.Option(
            prompt_required=False,
            help=f"User name, default to {CURRENT_USER_NAME}",
        ),
    ] = CURRENT_USER_NAME,
    priority: Annotated[
        int, typer.Option(prompt_required=False, help="Priority, default to 0")
    ] = 0,
    timeout: Annotated[
        int,
        typer.Option(
            prompt_required=False,
            help=f"Ensemble timeout, default to {config.ensemble.default_timeout}",
        ),
    ] = config.ensemble.default_timeout,
    runs: Annotated[
        int,
        typer.Option(
            prompt_required=False,
            help=f"Num of task runs, default to {config.ensemble.default_tasks}",
        ),
    ] = config.ensemble.default_tasks,
    fail_fast: Annotated[
        int,
        typer.Option(
            prompt_required=False,
            help=f"Maximum tolerable task fails, default to {config.ensemble.default_tolerable_failures}",
        ),
    ] = config.ensemble.default_tolerable_failures,
    tag: Annotated[
        str,
        typer.Option(
            prompt_required=False, help=f'Tag of the test package, default to ""'
        ),
    ] = "",
):
    from estelle.lib.model import create_ensemble

    create_ensemble(
        test_package, test_command, user, priority, timeout, runs, fail_fast, tag
    )


@_cli.command()
def mock_start(
    runs: Annotated[
        int,
        typer.Option(
            prompt_required=False,
            help=f"Num of task runs, default to {config.ensemble.default_tasks}",
        ),
    ] = 100000,
    fail_fast: Annotated[
        int,
        typer.Option(
            prompt_required=False,
            help=f"Maximum tolerable task fails, default to {config.ensemble.default_tolerable_failures}",
        ),
    ] = config.ensemble.default_tolerable_failures,
    fail_rate: Annotated[
        float, typer.Option(prompt_required=False, help=f"Fail rate")
    ] = 0.01,
):
    from estelle.lib.model import create_test_ensemble

    create_test_ensemble(
        owner=CURRENT_USER_NAME, runs=runs, fail_fast=fail_fast, fail_rate=fail_rate
    )


@_cli.command()
def pause(
    ensemble_identity: Annotated[List[str], typer.Argument(help="Pause an ensemble")]
):
    from estelle.lib.cli.ensemble import report_error
    from estelle.lib.model import pause_ensemble

    for identity in ensemble_identity:
        try:
            pause_ensemble(identity)
        except Exception as ex:
            report_error(identity, ex)


@_cli.command()
def resume(
    ensemble_identity: Annotated[List[str], typer.Argument(help="Resume an ensemble")]
):
    from estelle.lib.cli.ensemble import report_error
    from estelle.lib.model import resume_ensemble

    for identity in ensemble_identity:
        try:
            resume_ensemble(identity)
        except Exception as ex:
            report_error(identity, ex)


ENSEMBLE_STATE_DESCRIPTIONS = "\n\n".join(
    f"{item.value} - {item.name}" for item in EnsembleState
)


@_cli.command()
def inspect(
    ensemble_identity: Annotated[str, typer.Argument(help="Inspect an ensemble")]
):
    from estelle.lib.model import inspect_ensemble

    inspect_ensemble(ensemble_identity)


@_cli.command()
def list(
    status: Annotated[
        Optional[List[int]],
        typer.Option(
            prompt_required=False,
            help=f"""
State of the ensemble in integer:


{ENSEMBLE_STATE_DESCRIPTIONS}


If not present, show all ensembles.
""",
        ),
    ] = None,
    user: Annotated[
        Optional[str],
        typer.Option(
            prompt_required=False, help=f"User name, default to {CURRENT_USER_NAME}"
        ),
    ] = CURRENT_USER_NAME,
):
    from estelle.lib.cli.ensemble import ensemble_table
    from estelle.lib.model import list_ensemble

    if status is not None:
        status = tuple(EnsembleState(s) for s in status)

    with ensemble_table() as table_row_appender:
        for ensemble_item in list_ensemble(status, user):
            table_row_appender(ensemble_item)


@_cli.command()
def kill(
    ensemble_identity: Annotated[List[str], typer.Argument(help="Kill an ensemble")]
):
    from estelle.lib.cli.ensemble import report_error
    from estelle.lib.model import kill_ensemble

    for identity in ensemble_identity:
        try:
            kill_ensemble(identity)
        except Exception as ex:
            report_error(identity, ex)


@_cli.command()
def agents(all: Annotated[bool, typer.Option(help="List all agents")] = False):
    from estelle.lib.agent import is_stale
    from estelle.lib.cli.agent import agent_table
    from estelle.lib.model import list_agent

    with agent_table() as table_row_appender:
        for agent_item in list_agent():
            if not all and is_stale(agent_item):
                continue
            table_row_appender(agent_item)


if __name__ == "__main__":
    logger.remove()
    logger.add(sys.stderr, level="WARNING")
    _cli()
