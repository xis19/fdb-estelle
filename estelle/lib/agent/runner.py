import abc
import datetime
import enum
import gzip
import io
import os
import pathlib
import subprocess
import tarfile
import tempfile
import threading
import time

from typing import Optional

from loguru import logger

from .task_list import task_list
from .agent_info import agent_info
from ..config import config
from ..context import Context
from ..record import record
from ..record.base import EnsembleMissingError, EnsembleNotRunnableError
from ..storage import get_storage
from ..utils import chdir


class TaskExecuteStage(enum.Enum):
    CONSTRUCT = 0
    SETUP = 1
    EXECUTE = 2
    TEARDOWN = 3
    FINAL = 4


class TaskExecutor(abc.ABC):

    def __init__(self, context: Context):
        assert isinstance(context, Context)

        self._context = context
        self._stage: TaskExecuteStage = TaskExecuteStage.CONSTRUCT

    @property
    def stage(self) -> TaskExecuteStage:
        return self._stage

    def setup(self):
        self._stage = TaskExecuteStage.SETUP
        self._setup()

    @abc.abstractmethod
    def _setup(self):
        raise NotImplementedError()

    def execute(self) -> int:
        self._stage = TaskExecuteStage.EXECUTE
        return self._execute()

    @abc.abstractmethod
    def _execute(self) -> int:
        raise NotImplementedError()

    def teardown(self):
        self._stage = TaskExecuteStage.TEARDOWN
        self._teardown()
        self._stage = TaskExecuteStage.FINAL

    @abc.abstractmethod
    def _teardown(self):
        raise NotImplementedError()

    def __enter__(self):
        self.setup()
        return self.execute

    def __exit__(self, exc, value, tb):
        self.teardown()


def _download_proc(context: Context, output_stream: io.BufferedWriter):
    checksum = get_storage().download(context, output_stream, 128 * 1024)
    if checksum != context.checksum:
        raise RuntimeError(
            f"Download failed with checksum error: expected {context.checksum}, actual {checksum}"
        )


def _decompress_data(work_directory: pathlib.Path, input_stream: io.BufferedReader):
    with gzip.GzipFile(fileobj=input_stream, mode="r") as gzip_stream:
        with tarfile.TarFile(fileobj=gzip_stream, mode="r") as tar_stream:
            tar_stream.extractall(work_directory)


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


class CorrectnessPackageExecutor(TaskExecutor):

    def __init__(self, context: Context, timeout: float):
        super().__init__(context=context)
        self._timeout = timeout
        self._failed: bool = False
        self._work_directory: Optional[tempfile.TemporaryDirectory] = None

    def _setup(self):
        self._work_directory = tempfile.TemporaryDirectory(prefix="estelle")
        _prepare_context(self._context, self._work_directory.name)

    def _execute(self) -> None:
        with chdir(self._work_directory.name):
            with open("stdout", mode="w") as stdout, open("stderr", mode="w") as stderr:
                try:
                    test_exec = subprocess.Popen(
                        executable="./joshua_test",
                        args=(),
                        stdout=stdout,
                        stderr=stderr,
                    )
                    test_exec.communicate(self._timeout)

                    return test_exec.returncode
                except subprocess.TimeoutExpired:
                    logger.info("joshua_test failed with timeout")
                    return None

    def _teardown(self):
        if self._failed:
            # Upload the context
            pass

        if self._work_directory is not None:
            self._work_directory.cleanup()


TASK_TIMEOUT: float = 1800


def task_runner():
    last_task_run_time: datetime.datetime = datetime.datetime.now()

    while True:
        task_pair = task_list.take_task()

        if task_pair is None:
            idle_time = datetime.datetime.now() - last_task_run_time
            if idle_time.seconds > config.agent.max_idle_time:
                logger.info(
                    f"Idle for {idle_time.seconds} seconds > {config.agent.max_idle_time}"
                )
                break

            time.sleep(config.agent.task_list_check_interval)
            continue

        task, context = task_pair

        # Simulates the execution
        logger.info(f"Task {task.identity}, ensemble {task.ensemble_identity}: start")
        last_task_run_time = datetime.datetime.now()

        try:
            record.ensemble_task.report_start_task(task)
        except EnsembleMissingError:
            logger.warning(f"Ensemble {task.ensemble_identity} is missing, deleted?")
            continue
        except EnsembleNotRunnableError:
            logger.warning(
                f"Ensemble {task.ensemble_identity} is not in a runnable state"
            )
            continue

        executor = CorrectnessPackageExecutor(context, TASK_TIMEOUT)
        try:
            exit_code: Optional[int] = None
            executor.setup()
            exit_code = executor.execute()
            executor.teardown()
        except Exception as ex:
            logger.error(
                f"Ensemble {task.ensemble_identity} task {task.identity}: failed at stage {executor.stage.name}, with exception {ex}"
            )

        record.ensemble_task.report_task_result(
            task_identity=task.identity,
            return_value=exit_code,
        )
        record.ensemble.try_update_final_state(identity=task.ensemble_identity)
        logger.info(
            f"Task {task.identity}, ensemble {task.ensemble_identity}: terminate {exit_code}"
        )
        if exit_code == 0:
            agent_info.task_succeed()
        else:
            agent_info.tasks_failed()
