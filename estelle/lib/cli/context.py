import contextlib

from rich.console import Console
from rich.live import Live
from rich.progress_bar import ProgressBar
from rich.text import Text

from ..context import Context
from ..model import get_context_by_identity


@contextlib.contextmanager
def context_download_progressbar(package_size: int):
    progress_bar = ProgressBar(total=package_size)

    def step(block_size: int) -> bool:
        completed = progress_bar.completed
        progress_bar.update(completed + block_size)
        return True

    with Live(progress_bar, refresh_per_second=1):
        yield step


def report_error(context_id: str, ex: Exception):
    console = Console()
    console.print(f"{context_id}: Exception {ex}")
    raise ex
