import contextlib

from rich.console import RenderableType
from rich.live import Live
from rich.table import Table
from rich.text import Text

from ..agent import Agent as AgentItem, is_stale
from ..utils import get_id_width, render_datetime


@contextlib.contextmanager
def agent_table():
    table = Table()
    table.add_column("ID", width=get_id_width())
    table.add_column("Host")
    table.add_column("Create Time", justify="left")
    table.add_column("Status", justify="left")
    table.add_column("Success", justify="right", width=12)
    table.add_column("Failure", justify="right", width=12)

    def render_status(agent: AgentItem) -> RenderableType:
        if is_stale(agent):
            return Text("STALE", "red bold")
        else:
            return Text("ACTIVE", "green bold")

    def append(agent: AgentItem) -> RenderableType:
        table.add_row(
            agent.identity[: get_id_width()],
            agent.hostname,
            render_datetime(agent.start_time),
            render_status(agent),
            Text(f"{agent.tasks_succeed}"),
            Text(f"{agent.tasks_failed}"),
        )

    with Live(table, refresh_per_second=1):
        yield append
