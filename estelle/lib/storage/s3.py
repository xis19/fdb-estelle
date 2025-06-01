import io
import os
import pathlib

import boto3

from typing import Optional, Dict, Any

from loguru import logger

from .base import DownloaderBase, UploaderBase
from ..context import Context


class Uploader(UploaderBase):
    def __init__(self, context: Context, s3_bucket: str, s3_key_base: str):
        super().__init__(context)

        self._client = boto3.client("s3")
        self._s3_bucket_name = s3_bucket

        self._session: Optional[Dict[str, Any]] = None
        self._s3_key: pathlib.Path = pathlib.Path(s3_key_base).joinpath(
            context.identity
        )

    @property
    def s3_key(self) -> pathlib.Path:
        return self._s3_key

    def _prepare(self):
        self._session = self._client.create_multipart_upload(
            Bucket=self._s3_bucket_name, Key=self._s3_key
        )
        logger.info(f"Open {self._s3_bucket_name} / {self._s3_key} for writing")

    def _success(self):
        assert self._session is not None

        self._client.complete_multipart_upload(Bucket=self._s3_bucket_name, Key=self._s3_key, )
        logger.info(f"Close {self._s3_bucket_name} / {self._s3_key} for writing")

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
