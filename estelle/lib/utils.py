import contextlib
import datetime
import os
import pathlib
from typing import Optional, Union

from rich.console import RenderableType
from rich.text import Text

__LOCAL_TIMEZONE: Union[datetime.tzinfo, None] = (
    datetime.datetime.now(datetime.timezone.utc).astimezone().tzinfo
)


def to_local_timezone(utc_datetime: datetime.datetime) -> datetime.datetime:
    """Convert a UTC datetime object to local timezone

    NOTE: The returning datetime object has no timezone information
    """
    if __LOCAL_TIMEZONE is not None and (offset := __LOCAL_TIMEZONE.utcoffset(None)):
        return utc_datetime + offset
    else:
        return utc_datetime


def get_utc_datetime() -> datetime.datetime:
    """Gets current UTC datetime with no timezone"""
    return datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)


def render_datetime(utc_datetime: Optional[datetime.datetime]) -> RenderableType:
    """Renders a date time object to RenderableType"""
    if utc_datetime is not None:
        Text(to_local_timezone(utc_datetime).strftime("%Y/%m/%d %H:%M:%S"))
    return ""


__ID_WIDTH: int = 32


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
