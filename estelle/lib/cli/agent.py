import contextlib

from rich.console import RenderableType
from rich.live import Live
from rich.table import Table
from rich.text import Text

from ..agent import Agent as AgentItem
from ..agent import is_stale
from ..utils import get_id_width, render_datetime


@contextlib.contextmanager
def agent_table():
    table = Table()
    table.add_column("ID", width=get_id_width())
    table.add_column("Host")
    table.add_column("Create Time", justify="left")
    table.add_column("Last Heartbeat", justify="left")
    table.add_column("Current Ensemble", width=get_id_width())
    table.add_column("Current Task", width=get_id_width())
    table.add_column("Task Stage")

    def render_status(agent: AgentItem) -> RenderableType:
        if is_stale(agent):
            return Text("STALE", "red bold")
        else:
            return Text("ACTIVE", "green bold")

    def append(agent: AgentItem):
        table.add_row(
            agent.identity[: get_id_width()],
            agent.hostname,
            render_datetime(agent.start_time),
            render_datetime(agent.heartbeat),
            (agent.current_ensemble or "")[: get_id_width()],
            (agent.current_task or "")[: get_id_width()],
            agent.current_task_stage.name,
        )

    with Live(table, refresh_per_second=1):
        yield append
