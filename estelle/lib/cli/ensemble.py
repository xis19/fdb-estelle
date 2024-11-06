import contextlib

from rich.console import Console, RenderableType
from rich.live import Live
from rich.table import Table
from rich.text import Text

from ..record.base import (
    EnsembleMissingError,
    EnsembleStateInconsistentError,
)
from ..ensemble import Ensemble as EnsembleItem, EnsembleState
from ..utils import get_id_width, get_utc_datetime, render_datetime


@contextlib.contextmanager
def ensemble_table():
    table = Table()
    table.add_column("ID", width=get_id_width())
    table.add_column("Owner", justify="left")
    table.add_column("Create Time", justify="left")
    table.add_column("Status", justify="left")
    table.add_column("Progress", justify="right", width=35)
    table.add_column("Failures", justify="right", width=12)
    table.add_column("Timeout", justify="right", width=12)

    def render_status(ensemble: EnsembleItem) -> Text:
        if ensemble.state is EnsembleState.FAILED:
            style = "red bold"
        elif ensemble.num_failed > 0:
            style = "red"
        elif ensemble.state is EnsembleState.COMPLETED:
            style = "green bold"
        elif ensemble.state is EnsembleState.KILLED:
            style = "red bold"
        else:
            style = "green"
        return Text(ensemble.state.name, style=style)

    def render_progress(ensemble: EnsembleItem) -> RenderableType:
        return Text.assemble(
            (f"{ensemble.num_passed:7d}", "green"),
            (" / ", ""),
            (f"{ensemble.num_failed:3d}", "red"),
            (" / ", ""),
            (f"{ensemble.total_runs:7d}", "white"),
            (
                f"  ({(ensemble.num_failed + ensemble.num_passed)/(ensemble.total_runs) * 100:6.2f}%)"
            ),
        )

    def render_fails(ensemble: EnsembleItem) -> RenderableType:
        if ensemble.max_fails is None:
            return Text("-", style="bold")
        else:
            return Text.assemble(
                (
                    str(ensemble.num_failed),
                    "red bold" if ensemble.num_failed > 0 else "white",
                ),
                ("/", ""),
                (
                    str(ensemble.max_fails),
                    "red bold" if ensemble.num_failed > 0 else "white",
                ),
            )

    def render_timeout(ensemble: EnsembleItem) -> RenderableType:
        time_used = ensemble.time_used
        if ensemble.state is EnsembleState.RUNNABLE and ensemble.start_time is not None:
            time_used = time_used + (get_utc_datetime() - ensemble.start_time).seconds
        if ensemble.timeout is None:
            return Text("-", style="bold")
        else:
            return Text.assemble(
                (str(time_used or "0"), ""),
                ("/", ""),
                (
                    str(ensemble.timeout),
                    "red bold" if ensemble.time_used > ensemble.timeout else "white",
                ),
            )

    def append(ensemble: EnsembleItem) -> RenderableType:
        table.add_row(
            ensemble.identity[: get_id_width()],
            ensemble.owner,
            render_datetime(ensemble.create_time),
            render_status(ensemble),
            render_progress(ensemble),
            render_fails(ensemble),
            render_timeout(ensemble),
        )

    with Live(table, refresh_per_second=1):
        yield append


def report_error(ensemble_identity: str, ex: Exception):
    console = Console()
    prefix = ("ERROR: ", "red bold")
    if isinstance(ex, EnsembleStateInconsistentError):
        assert ex.identity.startswith(ensemble_identity)
        console.print(
            Text.assemble(
                prefix,
                ("Ensemble ", ""),
                (ex.identity, "bold"),
                (" expected state ", ""),
                (ex.expected.name, "yellow"),
                (" but got ", ""),
                (ex.actual.name, "red"),
            )
        )
    elif isinstance(ex, EnsembleMissingError):
        console.print(
            Text.assemble(
                prefix, ("Ensemble ", ""), (ex.identity, "bold"), (" does not exist")
            )
        )
    else:
        console.print(f"Unrecognized exception: {ex.__class__.__name__}")
        raise ex
