import io
import os
import pathlib

from typing import Optional

from loguru import logger
from platformdirs import user_cache_dir

from .base import DownloaderBase
from ..config import config
from ..context import Context


class Downloader(DownloaderBase):

    def __init__(self, context: Context, original_downloader: DownloaderBase):
        self._context = context
        self._cache_filename = config.storage.local_cache_directory.joinpath(
            context.identity + "-localcache"
        )

        self._use_cache: Optional[bool] = None
        self._original_downloader = original_downloader
        self._cache_reader: Optional[io.BufferedReader] = None
        self._cache_writer: Optional[io.BufferedWriter] = None

    def _prepare(self):
        if self._cache_filename.exists():
            self._use_cache = True
            self._cache_reader = open(self._cache_filename, "rb")
            logger.info(f"Using cached file {self._context.identity}")
        else:
            self._use_cache = False
            self._cache_writer = open(self._cache_filename, "wb")
            self._original_downloader._prepare()
            logger.info(f"File not cached, downloading and caching")

    def _download_part(self, buffer_size: int) -> bytes:
        if self._use_cache:
            return self._cache_reader.read(buffer_size)
        else:
            data = self._original_downloader._download_part(buffer_size)
            self._cache_writer.write(data)
            return data

    def _success(self):
        if self._use_cache:
            logger.debug(f"Close cache file reader {self._cache_filename}")
            self._cache_reader.close()
        else:
            logger.debug(f"Close cache file writer {self._cache_filename}")
            self._cache_writer.close()
            self._original_downloader._success()
