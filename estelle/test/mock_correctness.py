import gzip
import io
import tarfile

from loguru import logger


class MockCorrectnessBuilder:
    def __init__(self, fail_rate: float, test_time_min: float, test_time_max: float):
        self._fail_rate = fail_rate
        self._test_time_min = test_time_min
        self._test_time_max = test_time_max

    def _generate_joshua_test(self) -> bytes:
        return f"""\
#! /usr/bin/env python3

import random
import sys
import time

_fail_rate = {self._fail_rate}
_test_time_min = {self._test_time_min}
_test_time_max = {self._test_time_max}

def main():
    random.seed()

    print("Begin Joshua test")
    sleep_time = random.uniform(_test_time_min, _test_time_max)
    time.sleep(sleep_time)
    print("End Joshua test")

    if random.uniform(0, 1) < _fail_rate:
        print("Test failed")
        sys.exit(1)
    else:
        print("Test succeed")
        sys.exit(0)


if __name__ == '__main__':
    main()""".encode(
            "utf-8"
        )

    def _generate_joshua_timeout(self) -> bytes:
        return f"""\
#! /usr/bin/env python3

# Intended to be empty""".encode(
            "utf-8"
        )

    def _add_file(self, tar_stream: tarfile.TarFile, name: str, data: bytes):
        tarinfo = tarfile.TarInfo(name).replace(
            uid=1000,
            uname="joshua",
            gid=1000,
            gname="joshua",
            mode=0o755,
        )
        tarinfo.size = len(data)
        tar_stream.addfile(tarinfo=tarinfo, fileobj=io.BytesIO(data))

    def generate_mock_correctness(self) -> bytes:
        data: io.BytesIO = io.BytesIO()
        with gzip.GzipFile(fileobj=data, mode="w") as compress_stream:
            with tarfile.TarFile(fileobj=compress_stream, mode="w") as tar_stream:
                logger.info("Adding joshua_test")
                self._add_file(tar_stream, "joshua_test", self._generate_joshua_test())

                logger.info("Adding joshua_timeout")
                self._add_file(
                    tar_stream, "joshua_timeout", self._generate_joshua_timeout()
                )
        return data.getvalue()
