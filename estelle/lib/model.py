import io
import pathlib
from typing import Generator, Optional, Sequence, Union
from types import NoneType

from loguru import logger

from estelle.lib.context import Context
from estelle.lib.ensemble import Ensemble, EnsembleState
from estelle.lib.record import record
from estelle.lib.storage import get_storage, CallbackType as StorageCallbackType
from estelle.lib.task import TaskState

assert record is not None


def create_test_ensemble(owner: str, runs: int, fail_fast: int, fail_rate: float):
    from estelle.test.mock_correctness import MockCorrectnessBuilder

    correctness_tar_gz = MockCorrectnessBuilder(
        fail_rate=fail_rate, test_time_min=1, test_time_max=5
    ).generate_mock_correctness()
    correctness_tar_gz_size = len(correctness_tar_gz)
    logger.debug(
        f"Generated mock correctness.tar.gz with size {correctness_tar_gz_size}"
    )

    context = Context.new(
        owner=owner,
        size=correctness_tar_gz_size,
        checksum=None,
        tag="MockTestCorrectness",
    )

    result = get_storage().upload(context, io.BytesIO(correctness_tar_gz))
    context.checksum = result.checksum
    assert context.size == result.total_bytes
    record.context.insert(context)
    logger.debug(f"Uploaded correctness.tar.gz with checksum {result.checksum}")

    ensemble = Ensemble.new(
        owner=owner,
        total_runs=runs,
        context_identity=context.identity,
        executable="correctness",
        timeout=1800,
        max_fails=fail_fast,
    )
    record.ensemble.insert(ensemble)
    logger.debug(f"Inserted new ensemble {ensemble.identity}")


def create_ensemble(
    package: Union[pathlib.Path, str],
    package_size: int,
    user: str,
    timeout: int,
    runs: int,
    fail_fast: int,
    tag: str,
    callback: Optional[StorageCallbackType] = None,
):
    context = Context.new(owner=user, size=package_size, checksum=None, tag=tag)
    storage = get_storage()
    with open(package, "rb") as stream:
        result = storage.upload(context, stream, callback=callback)
    context.checksum = result.checksum
    assert package_size == result.total_bytes
    record.context.insert(context)

    ensemble = Ensemble.new(
        owner=user,
        total_runs=runs,
        context_identity=context.identity,
        # FIXME hardcoded
        executable="./joshua_test",
        timeout=timeout,
        max_fails=fail_fast,
    )
    record.ensemble.insert(ensemble)

    return ensemble.identity


def list_ensemble(
    state: Optional[Union[EnsembleState, Sequence[EnsembleState]]] = None,
    owner: Optional[str] = None,
) -> Generator[Ensemble, None, None]:
    if isinstance(state, EnsembleState):
        state = (state,)

    for item in record.ensemble.iterate(owner=owner, state=state):
        yield item


def list_task(
    ensemble_identity: str,
    state: Optional[TaskState] = None,
):
    for item in record.ensemble.iterate_tasks(ensemble_identity, state):
        yield item


def get_context_by_identity(identity: str) -> Optional[Context]:
    return record.context.get(identity)


def get_context_data(
    identity: str,
    output_path: Union[str, pathlib.Path],
    callback: Optional[StorageCallbackType] = None,
) -> Optional[pathlib.Path]:
    context = get_context_by_identity(identity)
    if context is None:
        raise KeyError(f"Context ID not found {identity}")

    with open(output_path, mode="wb") as stream:
        get_storage().download(context, stream, callback=callback)


def inspect_ensemble(identity: str):
    if not record.ensemble.exists(identity):
        raise KeyError(f"Ensemble {identity} not found")

    raise NotImplementedError()


def pause_ensemble(identity: str):
    record.ensemble.pause(identity)


def resume_ensemble(identity: str):
    record.ensemble.resume(identity)


def kill_ensemble(identity: str):
    logger.info(f"Killing {identity}")
    record.ensemble.kill(identity)


def list_agent():
    for item in record.agent.iterate():
        yield item
