import dataclasses
import pathlib
from typing import List, Optional, Callable, Protocol

import opendal
from loguru import logger

from ..config import config
from ..context import Context
from .checksum import Checksum


@dataclasses.dataclass
class IOResult:
    checksum: str
    total_bytes: int


class Readable(Protocol):
    def read(self, size: Optional[int], /) -> bytes:
        raise NotImplementedError()


class Writable(Protocol):
    def write(self, bs: bytes, /):
        raise NotImplementedError()


CallbackType = Callable[[int], bool]


class Storage:
    """Class for a database that stores context used by fdb-estelle"""

    def __init__(
        self,
        operator: opendal.Operator,
        local_cache_directory: Optional[pathlib.Path] = None,
    ):
        self._local_cache_directory: Optional[pathlib.Path] = None
        if local_cache_directory is not None:
            self._local_cache_directory = local_cache_directory
            self._local_cache_directory.mkdir(mode=0o655, parents=True, exist_ok=True)

        self._operator_ = operator

    @property
    def _operator(self) -> opendal.Operator:
        return self._operator_

    def upload(
        self,
        context: Context,
        reader: Readable,
        buffer_size: Optional[int] = None,
        callback: Optional[CallbackType] = None,
    ) -> IOResult:
        """Write the BLOB from the reader"""
        with self._operator.open(context.identity, "wb") as stream:
            return self._copy(reader, [stream], buffer_size, callback)

    def _is_locally_cached(self, context: Context) -> bool:
        if self._local_cache_directory is None:
            return False

        return self._local_cache_directory.joinpath(context.identity).exists()

    def _copy(
        self,
        source: Readable,
        writers: List[Writable],
        buffer_size: Optional[int] = None,
        callback: Optional[CallbackType] = None,
    ) -> IOResult:
        """Copy the BLOB"""
        buffer_size = buffer_size or config.storage.write_buffer_size
        checksum = Checksum()

        total_bytes = 0
        byte_data = source.read(buffer_size)
        while byte_data is not None and len(byte_data) != 0:
            block_size = len(byte_data)
            total_bytes += block_size
            for writer in writers:
                writer.write(byte_data)
            checksum.update(byte_data)
            if callback is not None:
                callback(block_size)
            byte_data = source.read(buffer_size)

        return IOResult(checksum=checksum.hexdigest, total_bytes=total_bytes)

    def download(
        self,
        context: Context,
        writer: Writable,
        buffer_size: Optional[int] = None,
        callback: Optional[CallbackType] = None,
    ) -> IOResult:
        """Read the BLOB by the identify into the writer"""
        if self._is_locally_cached(context):
            assert self._local_cache_directory is not None
            logger.info(f"Using local cache for context {context.identity}")
            with open(
                self._local_cache_directory.joinpath(context.identity), "rb"
            ) as source:
                return self._copy(source, [writer], buffer_size, callback)

        with self._operator.open(context.identity, "rb") as source:
            if self._local_cache_directory is None:
                logger.info(f"Downloading context {context.identity}")
                return self._copy(source, [writer], buffer_size)

            with open(
                self._local_cache_directory.joinpath(context.identity), "wb"
            ) as local_cache_writer:
                logger.info(f"Downloading and caching context {context.identity}")
                return self._copy(
                    source, [writer, local_cache_writer], buffer_size, callback
                )
