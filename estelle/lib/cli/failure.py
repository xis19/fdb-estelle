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
from ..context import Context as ContextItem
from ..utils import get_id_width, get_utc_datetime, render_datetime

@contextlib.contextmanager
def failure_table():
    table = Table()
    table.add_column("ID", width=get_id_width())
    table.add_column("Owner", justify="left")
    table.add_column("Size", justify="left")

    def append()