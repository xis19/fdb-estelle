import io
import os
import pathlib

from typing import Optional

from loguru import logger

from .base import DownloaderBase, UploaderBase
from ..config import config
from ..context import Context


class Uploader(UploaderBase):
    def __init__(self, context: Context, work_directory: pathlib.Path):
        super().__init__(context)

        self._work_directory = work_directory
        if not self._work_directory.exists():
            os.makedirs(self._work_directory)

        self._writer: Optional[io.BufferedWriter] = None
        self._filename: pathlib.Path = self._work_directory.joinpath(context.identity)

    def _prepare(self):
        self._writer = open(self._filename, "wb")
        logger.info(f"Open {self._filename} for writing")

    def _success(self):
        assert self._writer is not None

        self._writer.close()
        logger.info(f"Close {self._filename} for writing")

    def _upload_part(self, part: bytes):
        assert self._writer is not None

        self._writer.write(part)


class Downloader(DownloaderBase):
    def __init__(self, context: Context, work_directory: pathlib.Path):
        super().__init__(context)

        self._work_directory = work_directory
        if not self._work_directory.exists():
            os.makedirs(self._work_directory)

        self._reader: Optional[io.BufferedReader] = None
        self._filename = self._work_directory.joinpath(context.identity)

    def _prepare(self):
        self._reader = open(self._filename, "rb")
        logger.info(f"Open {self._filename} for writing")

    def _success(self):
        assert self._reader is not None

        self._reader.close()
        logger.info(f"Close {self._filename} for writing")

    def _download_part(self, buffer_size: int) -> bytes:
        assert self._reader is not None

        return self._reader.read(buffer_size)
