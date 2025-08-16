import datetime
import enum

import pytest

from loguru import logger

from ..lib.ensemble import Ensemble, EnsembleState
from ..lib.serde_bytes import (
    serialize,
    deserialize,
    serialize_by_type,
    deserialize_by_type,
)


class EnumExample(enum.IntEnum):
    VALUE = 2


@pytest.fixture
def ensemble():
    return Ensemble(
        identity="IDENTITY",
        owner="xis19",
        create_time=datetime.datetime.fromtimestamp(27182818).astimezone(
            datetime.timezone.utc
        ),
        start_time=datetime.datetime.fromtimestamp(31415926).astimezone(
            datetime.timezone.utc
        ),
        terminate_time=None,
        state=EnsembleState.STOPPED,
        state_last_modified_time=datetime.datetime.fromtimestamp(4000000).astimezone(
            datetime.timezone.utc
        ),
        total_runs=10000,
        context_identity="cid",
        executable="/bin/ls",
        timeout=None,
        time_used=1000,
        max_fails=1000,
    )


def test_serde_dataclass(ensemble):
    sdict = serialize(ensemble)
    deserialized = deserialize(Ensemble, sdict)

    assert ensemble == deserialized


def test_serde_bytes():
    values = [
        (1024, int),
        (1.5, float),
        ("test", str),
        (b"test", bytes),
        (False, bool),
        (None, type(None)),
        (
            datetime.datetime(2000, 1, 2, 12, 13, 14, tzinfo=datetime.timezone.utc),
            datetime.datetime,
        ),
        (EnumExample.VALUE, EnumExample),
    ]

    for value, t in values:
        serialized = serialize_by_type(value)
        assert value == deserialize_by_type(serialized, t)
