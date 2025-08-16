import datetime
import enum
import struct

from types import NoneType
from typing import (
    Dict,
    Any,
    ClassVar,
    Optional,
    Protocol,
    TypeVar,
    Union,
    List,
    Callable,
    Sequence,
)

import fdb

from loguru import logger

from .base import (
    AgentBase,
    ContextBase,
    EnsembleBase,
    EnsembleMissingError,
    EnsembleNotRunnableError,
    EnsembleStateInconsistentError,
    TaskBase,
    RecordBase,
    kwargs_verify,
)
from ..serde_bytes import serialize, deserialize, serialize_by_type, deserialize_by_type
from ..ensemble import Ensemble as EnsembleItem, EnsembleState
from ..context import Context as ContextItem
from ..agent import Agent as AgentItem
from ..task import Task as TaskItem, TaskState
from ..utils import get_utc_datetime

fdb.api_version(710)

_db = fdb.open()

_FDB_1P = struct.pack("<q", 1)
_FDB_1M = struct.pack("<q", -1)


def _decode_counter_value(v: bytes):
    return struct.unpack("<q", v)[0]


class DataclassProtocol(Protocol):
    __dataclass_fields__: ClassVar[Dict[str, Any]]
    identity: str


class UpsertType(enum.IntEnum):
    INSERT = 0
    UPSERT = 1
    UPDATE = 2


def get_today_timestamp() -> str:
    """Returns the current date as a timestamp in bytes format."""
    today = datetime.datetime.now(datetime.timezone.utc).date()
    return today.strftime("%Y%m%d")


def _write(tr, d, item: DataclassProtocol):
    subdir = d.create_or_open(tr, item.identity)
    for k, v in serialize(item).items():
        tr[subdir[k]] = v


def _upsert(
    tr, d, item: DataclassProtocol, upsert: UpsertType = UpsertType.UPDATE
) -> bool:
    if not d.exists(tr, item.identity) and upsert is UpsertType.UPDATE:
        logger.warning("Not inserting since upsert flag is set to be UPDATE")
        return False

    if d.exists(tr, item.identity) and upsert is UpsertType.INSERT:
        logger.warning("Not updating since upsert flag is set to be INSERT")
        return False

    _write(tr, d, item)

    return True


def _update_counter(tr, d, key: bytes | str, value: bytes):
    if tr[d[key]].present():
        tr.add(d[key], value)
    else:
        tr[d[key]] = value


def _increase_counter(tr, d, key: bytes | str):
    return _update_counter(tr, d, key, _FDB_1P)


def _decrease_counter(tr, d, key: bytes | str):
    return _update_counter(tr, d, key, _FDB_1M)


_T = TypeVar("_T", bound=DataclassProtocol)


def _get_impl(tr, subpath, t):
    raw = {}
    for key, value in tr.get_range_startswith(subpath.key()):
        _, key = fdb.tuple.unpack(key)  # type: ignore
        raw[key] = value

    return deserialize(t, raw)


def _get(tr, d, identity: Union[str, bytes], t: type[_T]) -> Optional[_T]:
    if not d.exists(tr, identity):
        return None

    subpath = d.open(tr, identity)
    return _get_impl(tr, subpath, t)


def _retire(
    tr,
    data_path,
    counter_path,
    retire_data_path,
    retire_counter_path,
    counter_key: str,
    identity: str,
):
    assert data_path.exists(tr, identity)

    timestamp = get_today_timestamp()
    retired_data_timestamped_path = retire_data_path.create_or_open(tr, timestamp)
    fdb.directory.move(  # type: ignore
        tr,
        data_path.open(tr, identity).get_path(),
        retired_data_timestamped_path.get_path() + (identity,),
    )
    _decrease_counter(tr, counter_path, counter_key)
    _increase_counter(
        tr, retire_counter_path.create_or_open(tr, timestamp), counter_key
    )


@fdb.transactional
def _retire_transactional(
    tr,
    data_path,
    counter_path,
    retire_data_path,
    retire_counter_path,
    counter_key: str,
    identity: str,
):
    _retire(
        tr,
        data_path,
        counter_path,
        retire_data_path,
        retire_counter_path,
        counter_key,
        identity,
    )


@fdb.transactional
def _scan(tr, d, t: type[_T]) -> List[_T]:
    result: List[_T] = []

    for key in d.list(tr):
        item = _get(tr, d, key, t)
        assert item is not None
        result.append(item)

    return result


def _filter(tr, d, t: type[_T], conditioner: Callable[[_T], bool]) -> List[_T]:
    return [item for item in _scan(tr, d, t) if conditioner(item)]  # type: ignore


def _count(tr, d, t: type[_T], conditioner: Callable[[_T], bool]) -> int:
    return len(_filter(tr, d, t, conditioner))


class Record(RecordBase):

    def __init__(self, top_path_name: str = "estelle"):
        super().__init__()

        self._db = _db
        self._top_path_name = top_path_name
        self._top_path = fdb.directory.create_or_open(_db, self._top_path_name)  # type: ignore
        self._retire_path_name = f"{self._top_path_name}-retire"
        self._retire_path = fdb.directory.create_or_open(_db, self._retire_path_name)  # type: ignore

    def purge(self):
        result = fdb.directory.remove_if_exists(self._db, self._top_path_name)  # type: ignore
        if result:
            logger.info(f"Purged record {self._top_path_name}")

        result = fdb.directory.remove_if_exists(self._db, self._retire_path_name)  # type: ignore
        if result:
            logger.info(f"Purged retired records {self._retire_path_name}")

    @property
    def ensemble(self) -> EnsembleBase:
        if self._ensemble is None:
            self._ensemble = Ensemble(self._top_path, self._retire_path)
        return self._ensemble

    @property
    def context(self) -> ContextBase:
        if self._context is None:
            self._context = Context(self._top_path, self._retire_path)
        return self._context

    @property
    def agent(self) -> AgentBase:
        if self._agent is None:
            self._agent = Agent(self._top_path, self._retire_path)
        return self._agent

    def _expire_retired_data(self):
        logger.info("Expiring retired data")
        # FIXME Implement this


class _PathMixin:

    def _create_paths(self, top_path, retire_path, path_name: str):
        self._data_path = top_path.create_or_open(_db, path_name)
        self._counter_path = top_path.create_or_open(_db, "counter")
        self._retire_data_path = retire_path.create_or_open(_db, path_name)
        self._retire_counter_path = retire_path.create_or_open(_db, "counter")


class Context(ContextBase, _PathMixin):
    @staticmethod
    def _filter_func(c: ContextItem, owner: Optional[str] = None):
        return owner is None or c.owner == owner

    @staticmethod
    @fdb.transactional
    def _insert_transactional(tr, data_path, counter_path, item: ContextItem):
        _upsert(tr, data_path, item, UpsertType.INSERT)
        _increase_counter(tr, counter_path, "context")

    @staticmethod
    @fdb.transactional
    def _get_transactional(tr, data_path, identity: str) -> Optional[ContextItem]:
        return _get(_db, data_path, identity, ContextItem)

    def __init__(self, top_path, retire_path):
        super().__init__()

        self._create_paths(top_path, retire_path, "context")

    def _insert(self, item: ContextItem):
        Context._insert_transactional(_db, self._data_path, self._counter_path, item)

    def _get(self, identity: str) -> Optional[ContextItem]:
        return Context._get_transactional(_db, self._data_path, identity)

    def _count(self, **kwargs) -> int:
        kwargs_verify(tuple(), kwargs)
        value = _db[self._counter_path["context"]]
        if value is None:
            return 0
        return _decode_counter_value(value)

    def _iterate(self, **kwargs):
        kwargs_verify(("owner"), kwargs)
        for item in _filter(
            _db,
            self._data_path,
            ContextItem,
            lambda t: Context._filter_func(t, kwargs.get("owner")),
        ):
            yield item

    def _exists(self, identity: str) -> bool:
        return self._data_path.exists(_db, identity)

    def _retire(self, identity: str):
        _retire_transactional(
            _db,
            self._data_path,
            self._counter_path,
            self._retire_data_path,
            self._retire_counter_path,
            "context",
            identity,
        )


class Task(TaskBase):

    def _num_total_transactional(self) -> int:
        return (
            self._tr[self._counter_path[b"passed"]]
            + self._tr[self._counter_path[b"failed"]]
            + self._tr[self._counter_path[b"running"]]
        )

    def __init__(self, ensemble_identity: str, tr, top_path, retire_path):
        super().__init__(ensemble_identity)

        self._tr = tr
        self._data_path = top_path.create_or_open(_db, "task").create_or_open(
            _db, ensemble_identity
        )
        self._counter_path = top_path.create_or_open(_db, "counter").create_or_open(
            _db, ensemble_identity
        )
        self._retire_data_path = retire_path.create_or_open(_db, "task")
        self._retire_counter_path = retire_path.create_or_open(_db, "counter")

    def num_passed(self) -> int:
        return self._tr[self._counter_path[b"passed"]]

    def num_failed(self) -> int:
        return self._tr[self._counter_path[b"failed"]]

    def num_running(self) -> int:
        return self._tr[self._counter_path[b"running"]]

    def num_total(self) -> int:
        return self._num_total_transactional()

    def _new_task(self, task: TaskItem):
        _upsert(
            self._tr,
            self._data_path.create_or_open(self._tr, "running"),
            task,
            UpsertType.INSERT,
        )
        _increase_counter(self._tr, self._counter_path, b"running")

    def _set_task_result(
        self,
        identity: str,
        return_value: Optional[int] = None,
        execution_context_identity: Optional[str] = None,
    ):
        if return_value == 0:
            result = b"passed"
            return_state = TaskState.SUCCEED
        elif return_value is None:
            result = b"timedout"
            return_state = TaskState.TIMEDOUT
        else:
            result = b"failed"
            return_state = TaskState.FAILED
        _update_counter(self._tr, self._counter_path, result, _FDB_1P)
        _update_counter(self._tr, self._counter_path, b"running", _FDB_1M)

        task_path = self._data_path.open(self._tr, "running").open(self._tr, identity)
        task_path[b"state"] = serialize_by_type(return_state)
        task_path[b"execution_context_identity"] = serialize_by_type(
            execution_context_identity
        )
        task_path[b"terminate_time"] = serialize_by_type(get_utc_datetime())
        task_path[b"return_value"] = serialize_by_type(return_value)

        fdb.directory.move(  # type: ignore
            self._tr,
            self._data_path.open(self._tr, "running")
            .open(self._tr, identity)
            .get_path(),
            self._data_path.create_or_open(self._tr, result.decode()).get_path()
            + (identity,),
        )

    def _retire(self):
        fdb.directory.move(  # type: ignore
            self._tr,
            self._data_path.get_path(),
            self._retire_data_path.create_or_open(
                self._tr, self._ensemble_identity
            ).get_path()
            + (self._ensemble_identity,),
        )
        fdb.directory.move(  # type: ignore
            self._tr,
            self._counter_path.get_path(),
            self._retire_counter_path.create_or_open(
                self._tr, self._ensemble_identity
            ).get_path()
            + (self._ensemble_identity,),
        )


class Ensemble(EnsembleBase, _PathMixin):

    @staticmethod
    @fdb.transactional
    def _add_task_transactional(
        tr, ensemble_identity, data_path, top_path, retire_path, args: str
    ) -> str:
        if not data_path.exists(tr, ensemble_identity):
            raise EnsembleMissingError(ensemble_identity)
        if tr[data_path[ensemble_identity][b"state"]] != serialize_by_type(
            EnsembleState.RUNNABLE
        ):
            raise EnsembleNotRunnableError(ensemble_identity)

        task_obj = Task(ensemble_identity, tr, top_path, retire_path)
        return task_obj.new_task(args)

    @staticmethod
    @fdb.transactional
    def _report_task_result_transactional(
        tr,
        ensemble_identity: str,
        task_identity: str,
        data_path,
        top_path,
        retire_path,
        return_value: Optional[int] = None,
        execution_context_identity: Optional[str] = None,
    ) -> EnsembleState:
        if not data_path.exists(tr, ensemble_identity):
            raise EnsembleMissingError(ensemble_identity)

        task_obj = Task(ensemble_identity, tr, top_path, retire_path)
        task_obj.set_task_result(
            task_identity, return_value, execution_context_identity
        )

        ensemble_item = _get(tr, data_path, ensemble_identity, EnsembleItem)
        assert ensemble_item is not None

        num_failed = task_obj.num_failed()
        num_passed = task_obj.num_passed()
        if (
            ensemble_item.max_fails is not None
            and num_failed >= ensemble_item.max_fails
        ):
            Ensemble._update_state_impl(
                tr, data_path, ensemble_identity, EnsembleState.FAILED
            )
            return EnsembleState.FAILED
        elif num_failed + num_passed >= ensemble_item.total_runs:
            Ensemble._update_state_impl(
                tr, data_path, ensemble_identity, EnsembleState.COMPLETED
            )
            return EnsembleState.COMPLETED

        return ensemble_item.state

    @staticmethod
    def _update_state_impl(
        tr,
        data_path,
        identity: str,
        new_state: EnsembleState,
        expected_old_state: Optional[
            Union[EnsembleState, Sequence[EnsembleState]]
        ] = None,
    ) -> EnsembleItem:
        expected_old_state = (
            (expected_old_state,)
            if isinstance(expected_old_state, EnsembleState)
            else expected_old_state
        )
        item = _get(tr, data_path, identity, EnsembleItem)

        if item is None:
            raise EnsembleMissingError(identity)

        if expected_old_state is not None and item.state not in expected_old_state:
            raise EnsembleStateInconsistentError(
                identity, expected_old_state, item.state
            )

        item.state = new_state
        item.state_last_modified_time = get_utc_datetime()
        _upsert(tr, data_path, item, UpsertType.UPSERT)

        return item

    @staticmethod
    @fdb.transactional
    def _update_state_transactional(
        tr,
        data_path,
        identity: str,
        new_state: EnsembleState,
        expected_old_state: Optional[Union[EnsembleState, Sequence[EnsembleState]]],
    ):
        return Ensemble._update_state_impl(
            tr, data_path, identity, new_state, expected_old_state
        )

    @staticmethod
    @fdb.transactional
    def _retire_transactional(
        tr,
        data_path,
        counter_path,
        retire_path,
        retire_counter_path,
        top_path,
        ensemble_identity,
    ):
        if not data_path.exists(tr, ensemble_identity):
            raise EnsembleMissingError(ensemble_identity)

        Task(ensemble_identity, tr, top_path, retire_path).retire()
        _retire(
            tr,
            data_path,
            counter_path,
            retire_path,
            retire_counter_path,
            counter_key="ensemble",
            identity=ensemble_identity,
        )

    @staticmethod
    @fdb.transactional
    def _get_transactional(
        tr, data_path, retire_path, identity: str
    ) -> Optional[EnsembleItem]:
        result = _get(tr, data_path, identity, EnsembleItem)
        if result is None:
            return None

        task_info = Task(identity, tr, data_path, retire_path)
        result.num_failed = task_info.num_failed()
        result.num_passed = task_info.num_passed()
        result.num_running = task_info.num_running()

        return result

    @staticmethod
    @fdb.transactional
    def _insert_transactional(tr, data_path, counter_path, item: EnsembleItem):
        _upsert(tr, data_path, item, UpsertType.INSERT)
        _increase_counter(tr, counter_path, b"ensemble")

    def __init__(self, top_path, retire_path):
        super().__init__()

        self._top_path = top_path
        self._retire_path = retire_path
        self._create_paths(top_path, retire_path, "ensemble")

    def _update_state(
        self,
        identity: str,
        new_state: EnsembleState,
        expected_old_state: Optional[
            Union[EnsembleState, Sequence[EnsembleState]]
        ] = None,
    ):
        return Ensemble._update_state_transactional(
            _db, self._data_path, identity, new_state, expected_old_state
        )

    def _insert(self, item: EnsembleItem):
        Ensemble._insert_transactional(_db, self._data_path, self._counter_path, item)

    def _exists(self, identity: str) -> bool:
        return self._data_path.exists(_db, identity)

    def _get(self, identity: str) -> Optional[EnsembleItem]:
        return Ensemble._get_transactional(
            _db, self._data_path, self._retire_path, identity
        )

    def _retire(self, identity: str):
        return Ensemble._retire_transactional(
            _db, self._data_path, self._retire_path, self._top_path, identity
        )

    @staticmethod
    def _filter_func(
        e: EnsembleItem,
        state: Optional[Union[EnsembleState, Sequence[EnsembleState]]] = None,
        owner: Optional[str] = None,
    ):
        if state is not None:
            if isinstance(state, EnsembleState):
                state = (state,)
            if e.state not in state:
                return False

        if owner is not None and e.owner != owner:
            return False

        return True

    def _count(self, **kwargs) -> int:
        kwargs_verify(("state", "owner"), kwargs)
        return _count(
            _db,
            self._data_path,
            EnsembleItem,
            lambda t: Ensemble._filter_func(
                t, kwargs.get("state"), kwargs.get("owner")
            ),
        )

    def _iterate(self, **kwargs):
        kwargs_verify(("state", "owner"), kwargs)
        for item in _filter(
            _db,
            self._data_path,
            EnsembleItem,
            lambda t: Ensemble._filter_func(
                t, kwargs.get("state"), kwargs.get("owner")
            ),
        ):
            yield item

    def _add_ensemble_task(self, ensemble_identity: str, args: str | NoneType) -> str:
        return Ensemble._add_task_transactional(
            _db,
            ensemble_identity,
            self._data_path,
            self._top_path,
            self._retire_path,
            args,
        )

    def _set_ensemble_task_result(
        self,
        ensemble_identity: str,
        task_identity: str,
        return_value: int | NoneType,
        execution_context_identity: str | NoneType = None,
    ):
        return Ensemble._report_task_result_transactional(
            _db,
            ensemble_identity,
            task_identity,
            self._data_path,
            self._top_path,
            self._retire_path,
            return_value,
            execution_context_identity,
        )


class Agent(AgentBase, _PathMixin):

    @staticmethod
    @fdb.transactional
    def _insert_transactional(tr, data_path, counter_path, item: AgentItem):
        _upsert(tr, data_path, item, UpsertType.INSERT)
        _increase_counter(tr, counter_path, "agent")

    @staticmethod
    @fdb.transactional
    def _heartbeat_transactional(tr, data_path, identity: str):
        item = _get(tr, data_path, identity, AgentItem)
        assert item is not None
        item.heartbeat = datetime.datetime.now(datetime.timezone.utc)
        _upsert(tr, data_path, item, UpsertType.UPSERT)

    @staticmethod
    @fdb.transactional
    def _get_transactional(tr, data_path, identity: str) -> Optional[AgentItem]:
        return _get(tr, data_path, identity, AgentItem)

    def __init__(self, top_path, retire_path):
        super().__init__()

        self._create_paths(top_path, retire_path, "agent")

    def _insert(self, item: AgentItem):
        Agent._insert_transactional(_db, self._data_path, self._counter_path, item)

    def _exists(self, identity: str) -> bool:
        return self._data_path.exists(_db, identity)

    def _get(self, identity: str) -> Optional[AgentItem]:
        return Agent._get_transactional(_db, self._data_path, identity)

    def _count(self, **kwargs) -> int:
        return len(_scan(_db, self._data_path, AgentItem))

    def _iterate(self, **kwargs):
        for item in _scan(_db, self._data_path, AgentItem):  # type: ignore
            yield item

    def _retire(self, identity: str):
        _retire_transactional(
            _db,
            self._data_path,
            self._counter_path,
            self._retire_data_path,
            self._retire_counter_path,
            "agent",
            identity,
        )

    def _heartbeat(self, identity: str):
        Agent._heartbeat_transactional(_db, self._data_path, identity)
