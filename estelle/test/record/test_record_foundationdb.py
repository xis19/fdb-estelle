import random
import time
import uuid
from typing import Any, Callable, ClassVar, Dict, Generic, Protocol, TypeVar

import pytest

from ...lib.agent import Agent
from ...lib.config import config
from ...lib.context import Context
from ...lib.ensemble import Ensemble, EnsembleState
from ...lib.record import Record
from ...lib.record.base import (
    EnsembleMissingError,
    EnsembleNotRunnableError,
    EnsembleStateInconsistentError,
)
from ...lib.task import Task


class _ItemProtocol(Protocol):
    __dataclass_fields__: ClassVar[Dict[str, Any]]
    identity: str


T = TypeVar("T", bound=_ItemProtocol)


from dataclasses import dataclass
from typing import Protocol


@pytest.fixture
def record():
    record = Record("test-estelle")
    record.purge()

    yield record


class _CRUDTestBase(Generic[T]):
    DEFAULT_NUM_ITEMS = 10

    def __init__(
        self,
        record_term,
        item_spawner: Callable[[int], T],
        num_items: int = DEFAULT_NUM_ITEMS,
    ):
        self._spawner = item_spawner
        self._record_term = record_term
        self._num_items = num_items
        self._items: Dict[str, T] = dict()

    def setup(self):
        for index in range(self._num_items):
            item = self._spawner(index)
            self._items[item.identity] = item

        for item in self._items.values():
            self._record_term.insert(item)

    def verify_inserted(self):
        for key, item in self._items.items():
            assert self._record_term.exists(key)
            assert self._record_term.get(key) == item

        assert self._record_term.count() == self._num_items

    def verify_iterate(self):
        count = 0
        for item in self._record_term.iterate():
            assert item.identity in self._items
            assert item == self._items[item.identity]
            count += 1
        assert count == self._num_items

    def verify_retire(self):
        num_to_remove = self._num_items // 2
        for _ in range(num_to_remove):
            key, _ = self._items.popitem()
            self._record_term.retire(key)
            assert not self._record_term.exists(key)
        assert self._record_term.count() == (self._num_items - num_to_remove)

    def run_tests(self):
        self.setup()
        self.verify_inserted()
        self.verify_iterate()
        self.verify_retire()


class _ContextCRUDTest(_CRUDTestBase[Context]):
    pass


@pytest.mark.skipif(
    config.record.backend_type != "foundationdb", reason="Only for FoundationDB backend"
)
def test_context(record):

    _ContextCRUDTest(
        record.context,
        lambda index: Context.new(
            "owner", uuid.uuid1().int % 1024, "checksum", f"tag{index}"
        ),
    ).run_tests()


class _AgentCRUDTest(_CRUDTestBase[Agent]):

    def verify_heartbeat(self):
        agent_item = list(self._items.values())[0]
        self._record_term.heartbeat(agent_item)
        heartbeat_1 = self._record_term.get(agent_item.identity).heartbeat
        time.sleep(2)
        self._record_term.heartbeat(agent_item)
        heartbeat_2 = self._record_term.get(agent_item.identity).heartbeat
        assert heartbeat_2 > heartbeat_1

    def run_tests(self):
        super().run_tests()
        self.verify_heartbeat()


@pytest.mark.skipif(
    config.record.backend_type != "foundationdb", reason="Only for FoundationDB backend"
)
def test_agent(record):
    _AgentCRUDTest(record.agent, lambda _: Agent.new()).run_tests()


class _EnsembleCRUDTest:

    def __init__(self, record_term: Record, num_ensembles: int, num_tasks: int):
        self._record_term = record_term
        self._num_ensembles = num_ensembles
        self._num_tasks = num_tasks
        self._ensembles: Dict[str, Ensemble] = dict()
        self._ensemble_tasks: Dict[str, Dict[str, str]] = dict()

    def create_ensembles(self):
        for index in range(self._num_ensembles):
            ensemble = Ensemble.new(
                owner=f"owner{index}",
                total_runs=20,
                context_identity=f"context{index}",
                executable=f"executable{index}",
                timeout=1800,
                max_fails=3,
            )
            self._ensembles[ensemble.identity] = ensemble
            self._record_term.ensemble.insert(ensemble)

        assert self._record_term.ensemble.count() == self._num_ensembles

    def ensemble_state_change_test(self):
        ensemble_identity = list(self._ensembles.keys())[0]

        def _assert_ensemble_state(state: EnsembleState):
            ensemble = self._record_term.ensemble.get(ensemble_identity)
            assert ensemble is not None
            assert ensemble.state is state

        _assert_ensemble_state(EnsembleState.RUNNABLE)

        self._record_term.ensemble.pause(ensemble_identity)
        _assert_ensemble_state(EnsembleState.STOPPED)

        with pytest.raises(EnsembleStateInconsistentError):
            self._record_term.ensemble.pause(ensemble_identity)

        self._record_term.ensemble.resume(ensemble_identity)
        _assert_ensemble_state(EnsembleState.RUNNABLE)

        with pytest.raises(EnsembleStateInconsistentError):
            self._record_term.ensemble.resume(ensemble_identity)

        self._record_term.ensemble.kill(ensemble_identity)
        _assert_ensemble_state(EnsembleState.KILLED)

        with pytest.raises(EnsembleStateInconsistentError):
            self._record_term.ensemble.kill(ensemble_identity)

    def ensemble_task_handler(self):
        _random = random.Random()
        while True:
            num_terminated_ensemble = 0
            runnable = []
            for ensemble_item in self._record_term.ensemble.iterate():
                assert ensemble_item is not None
                if ensemble_item.state is not EnsembleState.RUNNABLE:
                    num_terminated_ensemble += 1
                else:
                    runnable.append(ensemble_item.identity)
            if num_terminated_ensemble == self._num_ensembles:
                break

            ensemble_identity = _random.choice(runnable)
            ensemble_item = self._record_term.ensemble.get(ensemble_identity)
            assert ensemble_item is not None
            task_identity = self._record_term.ensemble.add_ensemble_task(
                ensemble_identity, args="test-args"
            )
            ensemble_item1 = self._record_term.ensemble.get(ensemble_identity)
            assert ensemble_item1 is not None
            assert ensemble_item1.num_running == ensemble_item.num_running + 1

            result = random.choice([0, 0, 0, 0, 0, 0, 1, None])
            self._record_term.ensemble.set_ensemble_task_result(
                ensemble_identity, task_identity, result
            )
            ensemble_item2 = self._record_term.ensemble.get(ensemble_identity)
            assert ensemble_item2 is not None
            assert ensemble_item2.num_running == ensemble_item.num_running
            match result:
                case 0:
                    assert ensemble_item2.num_passed == ensemble_item.num_passed + 1
                case 1:
                    assert ensemble_item2.num_failed == ensemble_item.num_failed + 1
                case None:
                    assert ensemble_item2.num_timedout == ensemble_item.num_timedout + 1


@pytest.mark.skipif(
    config.record.backend_type != "foundationdb", reason="Only for FoundationDB backend"
)
def test_ensemble_task(record):
    tester = _EnsembleCRUDTest(record, 10, 20)
    tester.create_ensembles()
    tester.ensemble_state_change_test()
    tester.ensemble_task_handler()
