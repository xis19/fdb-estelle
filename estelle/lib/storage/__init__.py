from .base import Storage
from ..config import config


def get_storage() -> Storage:
    backend = config.storage.backend_type
    if backend == "local_filesystem":
        from .local_filesystem import Uploader, Downloader
        from .cached_download import Downloader as CachedDownloader

        local_storage_directory = config.storage.local_storage_directory
        assert local_storage_directory is not None
        return Storage(
            lambda context: Uploader(context, local_storage_directory),
            lambda context: CachedDownloader(
                context, Downloader(context, local_storage_directory)
            ),
        )
    else:
        raise RuntimeError(f"Unsupported backend type {backend}")
