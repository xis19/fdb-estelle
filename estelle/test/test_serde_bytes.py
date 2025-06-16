import datetime

import pytest

from loguru import logger

from ..lib.ensemble import Ensemble, EnsembleState
from ..lib.serde_bytes import serialize, deserialize


@pytest.fixture
def ensemble():
    return Ensemble(
        identity="IDENTITY",
        owner="xis19",
        priority=9,
        create_time=datetime.datetime.fromtimestamp(27182818),
        start_time=datetime.datetime.fromtimestamp(31415926),
        terminate_time=None,
        state=EnsembleState.STOPPED,
        state_last_modified_time=datetime.datetime.fromtimestamp(4000000),
        total_runs=10000,
        num_passed=5000,
        num_failed=500,
        context_identity="cid",
        executable="/bin/ls",
        timeout=None,
        time_used=1000,
        max_fails=1000
    )


def test_serde(ensemble):
    sdict = serialize(ensemble)
    deserialized = deserialize(Ensemble, sdict)

    assert ensemble == deserialized