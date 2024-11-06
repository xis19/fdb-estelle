import contextlib
import datetime
import os
import pathlib

from rich.console import RenderableType
from rich.text import Text

__LOCAL_TIMEZONE: datetime.timezone = (
    datetime.datetime.now(datetime.timezone.utc).astimezone().tzinfo
)


def to_local_timezone(utc_datetime: datetime.datetime) -> datetime.datetime:
    """Convert a UTC datetime object to local timezone

    NOTE: The returning datetime object has no timezone information
    """
    return utc_datetime + __LOCAL_TIMEZONE.utcoffset(None)


def get_utc_datetime() -> datetime.datetime:
    """Gets current UTC datetime with no timezone"""
    return datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)


def render_datetime(utc_datetime: datetime.datetime) -> RenderableType:
    """Renders a date time object to RenderableType"""
    return Text(to_local_timezone(utc_datetime).strftime("%Y/%m/%d %H:%M:%S"))


__ID_WIDTH: int = 8


def get_id_width() -> int:
    return __ID_WIDTH


@contextlib.contextmanager
def chdir(path: pathlib.Path):
    old_pwd = os.getcwd()
    try:
        os.chdir(path)
        yield
    finally:
        os.chdir(old_pwd)
