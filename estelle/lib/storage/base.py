import dataclasses
import io
import pathlib


from typing import Optional, List, Union

import opendal

from loguru import logger

from .checksum import Checksum
from ..config import config
from ..context import Context


@dataclasses.dataclass
class IOResult:
    checksum: str
    total_bytes: int


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
        reader: Union[io.BufferedReader, io.BytesIO],
        buffer_size: Optional[int] = None,
    ) -> IOResult:
        """Write the BLOB from the reader"""
        with self._operator.open(context.identity, "w") as stream:
            return self._copy(reader, [stream], buffer_size)

    def _is_locally_cached(self, context: Context) -> bool:
        if self._local_cache_directory is None:
            return False

        return self._local_cache_directory.joinpath(context.identity).exists()

    def _copy(
        self,
        source: Union[opendal.File, io.BufferedReader, io.BytesIO],
        writers: List[Union[io.BufferedWriter, opendal.File]],
        buffer_size: Optional[int] = None,
    ) -> IOResult:
        """Copy the BLOB"""
        buffer_size = buffer_size or config.storage.write_buffer_size
        checksum = Checksum()

        total_bytes = 0
        byte_data = source.read(buffer_size)
        while len(byte_data) != 0:
            total_bytes += len(byte_data)
            for writer in writers:
                writer.write(byte_data)
            checksum.update(byte_data)
            byte_data = source.read(buffer_size)

        return IOResult(checksum=checksum.hexdigest, total_bytes=total_bytes)

    def download(
        self,
        context: Context,
        writer: io.BufferedWriter,
        buffer_size: Optional[int] = None,
    ) -> IOResult:
        """Read the BLOB by the identify into the writer"""
        if self._is_locally_cached(context):
            assert self._local_cache_directory is not None
            with open(
                self._local_cache_directory.joinpath(context.identity), "rb"
            ) as source:
                return self._copy(source, [writer], buffer_size)

        with self._operator.open(context.identity, "rb") as source:
            if self._local_cache_directory is None:
                return self._copy(source, [writer], buffer_size)

            with open(
                self._local_cache_directory.joinpath(context.identity), "wb"
            ) as local_cache_writer:
                return self._copy(source, [writer, local_cache_writer], buffer_size)
