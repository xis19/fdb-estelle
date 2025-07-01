from typing import Optional

from loguru import logger

from ..config import config
from .base import RecordBase


record: Optional[RecordBase] = None

if config.record.backend_type == "sql":
    from .sql import Record
    logger.info("Using SQLite backend")
elif config.record.backend_type == "foundationdb":
    from .foundationdb import Record
    logger.info("Using FoundationDB backend")
else:
    raise RuntimeError(f"Unsupported backend {config.record.backend_type}")

record = Record()
