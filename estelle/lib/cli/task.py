import contextlib

from rich.console import Console, RenderableType
from rich.live import Live
from rich.table import Table
from rich.text import Text

from ..task import Task as TaskItem
from ..task import TaskState
from ..utils import get_id_width, get_utc_datetime, render_datetime


@contextlib.contextmanager
def task_table(ensemble_id: str):
    table = Table(title=f"Ensemble {ensemble_id}")
    table.add_column("ID", width=get_id_width())
    table.add_column("Start Time", justify="left")
    table.add_column("Terminate Time", justify="left")
    table.add_column("Status", justify="left")
    table.add_column("Context", justify="right", width=get_id_width())

    def render_status(task: TaskItem) -> Text:
        style = ""
        if task.state is TaskState.FAILED:
            style = "red bold"
        elif task.state is TaskState.PASSED:
            style = "green"
        return Text(task.state.name, style=style)

    def append(task: TaskItem) -> RenderableType:
        terminate_time = ""
        if task.terminate_time is not None:
            terminate_time = render_datetime(task.terminate_time)
        context_identity = ""
        if task.execution_context_identity is not None:
            context_identity = task.execution_context_identity[: get_id_width()]
        table.add_row(
            task.identity[: get_id_width()],
            render_datetime(task.start_time),
            terminate_time,
            render_status(task),
            context_identity,
        )

    with Live(table, refresh_per_second=1):
        yield append
