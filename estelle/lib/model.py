import io
import pathlib

from typing import Optional, Sequence

from loguru import logger

from estelle.lib.context import Context
from estelle.lib.ensemble import Ensemble, EnsembleState
from estelle.lib.storage import get_storage
from estelle.lib.record import record


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
        priority=0,
        total_runs=runs,
        context_identity=context.identity,
        executable="correctness",
        timeout=1800,
        max_fails=fail_fast,
    )
    record.ensemble.insert(ensemble)
    logger.debug(f"Inserted new ensemble {ensemble.identity}")


def create_ensemble(
    test_package: pathlib.Path,
    test_command: pathlib.Path,
    user: str,
    priority: int,
    timeout: int,
    runs: int,
    fail_fast: int,
    tag: int,
):
    package_path = pathlib.Path(test_package)
    if not package_path.exists():
        raise FileNotFoundError(package_path)
    package_size = package_path.stat().st_size

    context = Context.new(owner=user, size=package_size, checksum=None, tag=tag)
    storage = get_storage()
    with open(package_path, "rb") as stream:
        result = storage.upload(context, stream)
    context.checksum = result.checksum
    assert package_size == result.total_bytes
    record.context.insert(context)

    ensemble = Ensemble.new(
        owner=user,
        priority=priority,
        total_runs=runs,
        context_identity=context.identity,
        executable=str(test_command),
        timeout=timeout,
        max_fails=fail_fast,
    )
    record.ensemble.insert(ensemble)


def list_ensemble(
    state: Optional[EnsembleState | Sequence[EnsembleState]] = None,
    user: Optional[str] = None,
):
    if isinstance(state, EnsembleState):
        state = (state,)
    states = tuple(e.name for e in (state or EnsembleState))

    for item in record.ensemble.iterate(owner=user, state=states):
        yield item


def get_context_by_identity(identity: str) -> Optional[Context]:
    return record.context.get(identity)


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
