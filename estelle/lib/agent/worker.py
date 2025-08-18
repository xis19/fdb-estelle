import abc
import datetime
import glob
import io
import json
import os
import pathlib
import random
import subprocess
import tempfile
import threading
import time
from typing import Optional

from loguru import logger

from ..config import config
from ..context import Context
from ..ensemble import Ensemble, EnsembleState
from ..model import list_ensemble
from ..record import record
from ..storage import get_storage
from ..utils import chdir
from . import TaskExecuteStage
from .agent_info import agent_info

assert record is not None


_EPOCH_ORIGIN = datetime.datetime.fromtimestamp(0, datetime.timezone.utc)


def _get_ensemble() -> Optional[Ensemble]:
    """Gets an ensemble to run"""
    candidates = tuple(list_ensemble(EnsembleState.RUNNABLE))
    if len(candidates) > 0:
        now = datetime.datetime.now(datetime.timezone.utc)
        return random.choices(
            candidates,
            weights=[
                (now - (candidate.start_time or _EPOCH_ORIGIN)).seconds
                for candidate in candidates
            ],
            k=1,
        )[0]

    return None


_BLOCK_SIZE = 128 * 1024


class TaskExecutor(abc.ABC):

    def __init__(self, ensemble_identity: str, context: Context):
        assert isinstance(context, Context)

        self._ensemble_identity = ensemble_identity
        self._context = context
        agent_info.set_current_task_stage(TaskExecuteStage.CONSTRUCT)

    def setup(self):
        agent_info.set_current_task_stage(TaskExecuteStage.SETUP)
        self._setup()

    @abc.abstractmethod
    def _setup(self):
        raise NotImplementedError()

    def execute(self) -> Optional[int]:
        agent_info.set_current_task_stage(TaskExecuteStage.EXECUTE)
        return self._execute()

    @abc.abstractmethod
    def _execute(self) -> Optional[int]:
        raise NotImplementedError()

    def teardown(self):
        agent_info.set_current_task_stage(TaskExecuteStage.TEARDOWN)
        self._teardown()
        agent_info.set_current_task_stage(TaskExecuteStage.IDLE)

    @abc.abstractmethod
    def _teardown(self):
        raise NotImplementedError()

    def __enter__(self):
        self.setup()
        return self.execute

    def __exit__(self, exc, value, tb):
        self.teardown()


def _download_proc(context: Context, output_stream: io.BufferedWriter):
    result = get_storage().download(context, output_stream, 128 * 1024)
    if result.checksum != context.checksum:
        raise RuntimeError(
            f"Download failed with checksum error: expected {context.checksum}, actual {result.checksum}"
        )
    if result.total_bytes != context.size:
        raise RuntimeError(
            f"Download failed with inconsistent size: expected {context.size}, actual {result.total_bytes}"
        )


def _decompress_data(work_directory: pathlib.Path, input_stream: io.BufferedReader):
    # TODO Cache the decompressed data
    with chdir(work_directory):
        with subprocess.Popen(
            ["tar", "xz"], executable="tar", stdin=subprocess.PIPE
        ) as tar:
            assert tar.stdin is not None
            logger.info(f"Decompressing from stream: {work_directory}")
            num_bytes = 0
            while data := input_stream.read(_BLOCK_SIZE):
                tar.stdin.write(data)
                num_bytes += len(data)
            logger.info(f"Decompressed {num_bytes} bytes")

    # Python builtin tar.gz is *EXTREMELY* slow
    # with gzip.GzipFile(fileobj=input_stream, mode="r") as gzip_stream:
    #     with tarfile.TarFile(fileobj=gzip_stream, mode="r") as tar_stream:
    #         tar_stream.extractall(work_directory)


def _prepare_context(context: Context, work_directory: pathlib.Path):
    logger.info(f"Preparing context {context.identity} into {work_directory}")

    rd, wr = os.pipe()

    download_thread = threading.Thread(
        target=_download_proc, args=(context, io.FileIO(file=wr, mode="w"))
    )
    download_thread.start()

    decompress_thread = threading.Thread(
        target=_decompress_data, args=(work_directory, io.FileIO(file=rd, mode="r"))
    )
    decompress_thread.start()

    decompress_thread.join()


def _pack_execute_context(
    work_directory: pathlib.Path, output_stream: io.BufferedWriter
):
    with chdir(work_directory):
        file_list = list(glob.glob("./**"))
        num_bytes = 0
        with subprocess.Popen(
            ["tar", "cz"] + file_list,
            executable="tar",
            stdout=output_stream,
            stderr=subprocess.PIPE,
        ) as tar:
            assert tar.stdout is not None
            logger.info(f"Packing execute context from {work_directory}")
            while data := tar.stdout.read(_BLOCK_SIZE):
                output_stream.write(data)
                num_bytes += len(data)
        logger.info(f"Packed {num_bytes} bytes")

        # Python builtin tar.gz is *EXTREMELY* slow
        # with gzip.GzipFile(fileobj=output_stream, mode="w") as gzip_stream:
        #     with tarfile.TarFile(fileobj=gzip_stream, mode="w") as tar_stream:
        #         for item in glob.glob("./**"):
        #             logger.debug(f"Adding {item} to execute context")
        #             tar_stream.add(item, recursive=False)


def _upload_execute_context(
    task_identity: str, task_context: Context, work_directory: pathlib.Path
) -> Context:
    assert isinstance(task_context, Context)
    logger.info(f"Packing the execute context in {work_directory}")

    rd, wr = os.pipe()

    pack_thread = threading.Thread(
        target=_pack_execute_context,
        args=(work_directory, io.FileIO(file=wr, mode="wb")),
    )
    pack_thread.start()

    context = Context.new(
        owner=task_context.owner,
        size=0,
        checksum="",
        tag=json.dumps({"task": task_identity, "context": task_context.identity}),
    )
    result = get_storage().upload(context, io.FileIO(file=rd, mode="rb"))
    context.checksum = result.checksum
    context.size = result.total_bytes
    record.context.insert(context)

    pack_thread.join()
    logger.info(f"Uploaded context {context.identity}")

    return context


class CorrectnessPackageExecutor(TaskExecutor):

    def __init__(
        self,
        ensemble_identity: str,
        context: Context,
        task_identity: str,
        timeout: float,
    ):
        super().__init__(ensemble_identity=ensemble_identity, context=context)
        self._timeout = timeout
        self._failed: bool = False
        self._task_identity = task_identity
        self._work_directory: Optional[tempfile.TemporaryDirectory] = None

    def _setup(self):
        self._work_directory = tempfile.TemporaryDirectory(prefix="estelle")
        _prepare_context(self._context, self._work_directory.name)

    def _execute(self) -> Optional[int]:
        assert self._work_directory is not None
        with chdir(self._work_directory.name):
            logger.info(f"Running test at {self._work_directory.name}")
            with open("stdout", mode="w") as stdout, open("stderr", mode="w") as stderr:
                try:
                    test_exec = subprocess.Popen(
                        executable="./joshua_test",
                        args=tuple(),
                        stdout=stdout,
                        stderr=stderr,
                    )
                    test_exec.communicate(timeout=self._timeout)

                    self._failed = test_exec.returncode != 0
                    return test_exec.returncode
                except subprocess.TimeoutExpired:
                    self._failed = True
                    logger.info("joshua_test failed with timeout")
                    return -1

    def _teardown(self):
        if self._failed:
            assert self._work_directory is not None
            _upload_execute_context(
                self._task_identity, self._context, self._work_directory.name
            )

        if self._work_directory is not None:
            self._work_directory.cleanup()


def run_task():
    ensemble_identity = agent_info.get_current_ensemble_identity()
    assert ensemble_identity is not None
    logger.info(f"Using ensemble {ensemble_identity}")
    record.ensemble.add_ensemble_task(ensemble_identity, "")

    ensemble_item = record.ensemble.get(ensemble_identity)
    assert ensemble_item is not None
    ensemble_context_item = record.context.get(ensemble_item.context_identity)
    assert ensemble_context_item is not None

    task_identity = record.ensemble.add_ensemble_task(
        ensemble_identity=ensemble_identity, args=""
    )
    agent_info.set_current_task(task_identity)
    executor = CorrectnessPackageExecutor(
        ensemble_identity,
        ensemble_context_item,
        task_identity,
        config.agent.task_timeout,
    )
    return_value: Optional[int] = None
    try:
        executor.setup()
        return_value = executor.execute()
        executor.teardown()
    except Exception as ex:
        logger.error(
            f"Ensemble {ensemble_identity} task {task_identity} exception {ex}"
        )

    record.ensemble.set_ensemble_task_result(
        ensemble_identity=ensemble_identity,
        task_identity=task_identity,
        return_value=return_value,
        execution_context_identity=executor._context.identity,
    )
    logger.info(
        f"Ensemble {ensemble_identity} task {task_identity}: terminate {return_value}"
    )
    if return_value == 0:
        agent_info.task_succeed()
    else:
        agent_info.tasks_failed()


def worker():
    task_count = 0
    last_task_terminate = datetime.datetime.now()
    while True:
        ensemble = _get_ensemble()
        if ensemble is None:
            time.sleep(config.agent.task_list_check_interval)
            if (
                datetime.datetime.now() - last_task_terminate
            ).seconds > config.agent.max_idle_time:
                break
            continue

        agent_info.set_current_ensemble(ensemble.identity)
        run_task()
        last_task_terminate = datetime.datetime.now()

        task_count += 1
        if task_count > config.agent.retire_after_task:
            logger.info(f"Retiring agent after completing {task_count} tasks")
            break
