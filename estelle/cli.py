import getpass
import pathlib
import sys
from typing import List, Optional

import typer
from loguru import logger
from typing_extensions import Annotated

from estelle.lib.config import config
from estelle.lib.ensemble import EnsembleState
from estelle.lib.task import TaskState

_cli = typer.Typer()
_list = typer.Typer()
_get = typer.Typer()
_cli.add_typer(_list, name="list", help="Lists simulation related objects")
_cli.add_typer(_get, name="get", help="Get objects")

CURRENT_USER_NAME = getpass.getuser()


@_cli.command()
def start(
    package: Annotated[
        pathlib.Path,
        typer.Option(help="Package contains the test context, e.g. correctness.tar.gz"),
    ],
    user: Annotated[
        str,
        typer.Option(
            prompt_required=False,
            help=f"User name, default to {CURRENT_USER_NAME}",
        ),
    ] = CURRENT_USER_NAME,
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
    """Starts an ensemble simulation"""
    from estelle.lib.model import create_ensemble
    from estelle.lib.cli.context import context_download_progressbar
    from rich.console import Console

    package_path = pathlib.Path(package)
    if not package_path.exists():
        raise FileNotFoundError(package_path)
    package_size = package_path.stat().st_size

    with context_download_progressbar(package_size) as step:
        ensemble_id = create_ensemble(
            package, package_size, user, timeout, runs, fail_fast, tag, callback=step
        )
    Console().print(f"Created ensemble [green bold]{ensemble_id}")


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
    """Creates an ensemble using test correctness package"""
    from estelle.lib.model import create_test_ensemble

    create_test_ensemble(
        owner=CURRENT_USER_NAME, runs=runs, fail_fast=fail_fast, fail_rate=fail_rate
    )


@_cli.command()
def pause(
    ensemble_identity: Annotated[List[str], typer.Argument(help="Ensemble ID")],
):
    """Pause an ensemble"""
    from estelle.lib.cli.ensemble import report_error
    from estelle.lib.model import pause_ensemble

    for identity in ensemble_identity:
        try:
            pause_ensemble(identity)
        except Exception as ex:
            report_error(identity, ex)


@_cli.command()
def resume(
    ensemble_identity: Annotated[List[str], typer.Argument(help="Ensemble ID")],
):
    """Resumes an ensemble"""
    from estelle.lib.cli.ensemble import report_error
    from estelle.lib.model import resume_ensemble

    for identity in ensemble_identity:
        try:
            resume_ensemble(identity)
        except Exception as ex:
            report_error(identity, ex)


_ENSEMBLE_STATE_DESCRIPTIONS = "\n\n".join(
    f"{item.value} - {item.name}" for item in EnsembleState
)
_TASK_STATE_DESCRIPTIONS = "\n\n".join(
    f"{item.value} - {item.name}" for item in TaskState
)


@_list.command(name="ensembles")
def list_ensemble(
    state: Annotated[
        Optional[List[int]],
        typer.Option(
            prompt_required=False,
            help=f"""
State of the ensemble in integer:


{_ENSEMBLE_STATE_DESCRIPTIONS}


If not present, show all ensembles.
""",
        ),
    ] = None,
    owner: Annotated[
        Optional[str],
        typer.Option(
            prompt_required=False, help=f"User name, default to {CURRENT_USER_NAME}"
        ),
    ] = CURRENT_USER_NAME,
):
    """Lists recent ensembles"""
    from estelle.lib.cli.ensemble import ensemble_table
    from estelle.lib.model import list_ensemble

    if state is not None:
        state_ = [EnsembleState(s) for s in state]
    else:
        state_ = None

    with ensemble_table() as table_row_appender:
        for ensemble_item in list_ensemble(state=state_, owner=owner):
            table_row_appender(ensemble_item)


@_list.command(name="agents")
def list_agents():
    """Lists recent agents"""
    from estelle.lib.agent import is_stale
    from estelle.lib.cli.agent import agent_table
    from estelle.lib.model import list_agent

    with agent_table() as table_row_appender:
        for agent_item in list_agent():
            if is_stale(agent_item):
                continue
            table_row_appender(agent_item)


@_list.command(name="failures")
def list_failures(ensemble_id: Annotated[str, typer.Argument(help="Ensemble ID")]):
    """List all test failures in the given ensemble"""
    from estelle.lib.cli.task import task_table
    from estelle.lib.model import list_task

    with task_table(ensemble_id) as table_row_appender:
        for state in (TaskState.FAILED, TaskState.TIMEDOUT):
            for task_item in list_task(ensemble_identity=ensemble_id, state=state):
                table_row_appender(task_item)


@_get.command(name="context")
def get_context(
    context_id: Annotated[str, typer.Argument(help="Context ID")],
    filename: Annotated[
        str, typer.Option(help="Output file name (stdout not supported)")
    ],
):
    """Download a given context"""
    from estelle.lib.model import get_context_by_identity, get_context_data
    from estelle.lib.cli.context import context_download_progressbar, report_error

    context = get_context_by_identity(context_id)

    try:
        if context is None:
            raise KeyError(f"Context {context_id} not found")
        if context.size is None:
            raise ValueError(f"Context {context_id} size unavailable")

        with context_download_progressbar(context.size) as step:
            get_context_data(context_id, filename, step)
    except Exception as ex:
        report_error(context_id, ex)


@_get.command(name="task")
def get_task(
    ensemble_id: Annotated[str, typer.Argument(help="Ensemble ID")],
    task_id: Annotated[str, typer.Argument(help="Task ID")],
):
    """Get the information of a given task"""
    from estelle.lib.model import get_ensemble_task
    from estelle.lib.cli.task import task_details_table

    task = get_ensemble_task(ensemble_id, task_id)
    task_details_table(task)


@_cli.command()
def kill(
    ensemble_identity: Annotated[List[str], typer.Argument(help="Kill an ensemble")],
):
    """Kills an ensemble simulation"""
    from estelle.lib.cli.ensemble import report_error
    from estelle.lib.model import kill_ensemble

    for identity in ensemble_identity:
        try:
            kill_ensemble(identity)
        except Exception as ex:
            report_error(identity, ex)


if __name__ == "__main__":
    logger.remove()
    logger.add(sys.stderr, level="WARNING")
    _cli()
