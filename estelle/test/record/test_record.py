import pytest

from ...lib.record import record


def test_context():
    from ...lib.context import Context

    record.context.insert(Context("abc", "me", 100, None, "tag0"))

    r = record.context.get("abc")
