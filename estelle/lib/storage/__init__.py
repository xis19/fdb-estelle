import opendal

from .base import Storage
from ..config import config


def get_storage() -> Storage:
    backend = config.storage.backend_type
    if backend == "local_filesystem":
        # No caching required
        return Storage(
            opendal.Operator("fs", root=str(config.storage.local_storage_directory))
        )
    elif backend == "s3":
        return Storage(
            opendal.Operator(
                "s3", bucket=config.storage.s3_bucket, region=config.storage.aws_region
            ),
            local_cache_directory=config.storage.local_cache_directory,
        )
    else:
        raise RuntimeError(f"Unsupported backend type {backend}")
