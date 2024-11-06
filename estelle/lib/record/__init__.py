from typing import Optional

from ..config import config
from .base import RecordBase


record: Optional[RecordBase] = None

if config.record.backend_type == "sql":
    from .sql import Record

    record = Record()
else:
    raise RuntimeError(f"Unsupported backend {config.record.backend_type}")
