from typing import Optional

from loguru import logger

from ..config import config
from .base import RecordBase

if config.record.backend_type == "sql":
    raise RuntimeError("SQL backend is not supported in this version of Estelle.")

    from .sql import Record

    logger.info("Using SQLite backend")
elif config.record.backend_type == "foundationdb":
    from .foundationdb import Record

    logger.info("Using FoundationDB backend")
else:
    raise RuntimeError(f"Unsupported backend {config.record.backend_type}")

record: RecordBase = Record()
