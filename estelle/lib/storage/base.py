import abc
import io

from typing import Callable, Optional, Type
from types import TracebackType

from loguru import logger

from .checksum import Checksum
from ..config import config
from ..context import Context


class PartOpsBase(abc.ABC):
    """Base class for part-wise ops"""

    def prepare(self):
        self._prepare()

    def finalize(self):
        self._finalize()

    def success(self):
        self._success()

    def failure(self, ex: BaseException):
        self._failure(ex)

    @abc.abstractmethod
    def _part_ops_generator(self, *args, **kwargs) -> Callable:
        raise NotImplementedError()

    def _prepare(self):
        return

    def _finalize(self):
        return

    def _failure(self, ex: BaseException):
        logger.error(f"{self.__class__.__name__}: Failure {ex}")
        raise ex

    def _success(self):
        return

    def __enter__(self) -> Callable:
        self.prepare()
        return self._part_ops_generator()

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        value: Optional[BaseException],
        _: Optional[TracebackType],
    ):
        if exc_type is None:
            self.success()
        else:
            self.failure(value)
        self.finalize()


class UploaderBase(PartOpsBase):
    """Base class for context uploader"""

    def __init__(self, context: Context):
        self._context = context

    def _part_ops_generator(self):
        return self._upload_part

    @abc.abstractmethod
    def _upload_part(self, part: bytes):
        raise NotImplementedError

    def _failure(self, ex: BaseException):
        logger.exception(f"Upload failed with exception {ex.__class__}")
        raise ex


class DownloaderBase(PartOpsBase):
    """Base class for context downloader"""

    def __init__(self, context: Context):
        self._context = context

    def _part_ops_generator(self):
        return self._download_part

    @abc.abstractmethod
    def _download_part(self, buffer_size: int) -> bytes:
        raise NotImplementedError()

    def _failure(self, ex: Exception):
        logger.exception(f"Download failed with exception {ex.__class__}")
        raise ex


class Storage:
    """Class for a database that stores context used by fdb-estelle"""

    def __init__(
        self,
        uploader_factory: Callable[[Context], UploaderBase],
        downloader_factory: Callable[[Context], DownloaderBase],
    ):
        self._uploader_factory = uploader_factory
        self._downloader_factory = downloader_factory

    def _get_uploader(self, context: Context) -> UploaderBase:
        return self._uploader_factory(context)

    def _get_downloader(self, context: Context) -> DownloaderBase:
        return self._downloader_factory(context)

    def upload(
        self,
        context: Context,
        reader: io.BufferedReader,
        buffer_size: Optional[int] = None,
    ) -> str:
        """Write the BLOB from the reader"""
        buffer_size = buffer_size or config.storage.read_buffer_size
        checksum = Checksum()

        with self._get_uploader(context) as upload_part:
            byte_data = reader.read(buffer_size)
            while len(byte_data) != 0:
                upload_part(byte_data)
                checksum.update(byte_data)
                byte_data = reader.read(buffer_size)

        return checksum.hexdigest

    def download(
        self,
        context: Context,
        writer: io.BufferedWriter,
        buffer_size: Optional[int] = None,
    ) -> str:
        """Read the BLOB by the identify into the writer"""
        buffer_size = buffer_size or config.storage.write_buffer_size
        checksum = Checksum()

        with self._get_downloader(context) as download_part:
            byte_data = download_part(buffer_size)
            while len(byte_data) != 0:
                writer.write(byte_data)
                checksum.update(byte_data)
                byte_data = download_part(buffer_size)

        return checksum.hexdigest
