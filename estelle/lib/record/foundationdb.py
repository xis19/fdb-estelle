import datetime
import enum
import struct
from types import NoneType
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    Generator,
    List,
    Optional,
    Protocol,
    Sequence,
    Tuple,
    TypeVar,
    Union,
)

import fdb
from loguru import logger

from ..agent import Agent as AgentItem
from ..context import Context as ContextItem
from ..ensemble import Ensemble as EnsembleItem
from ..ensemble import EnsembleState
from ..serde_bytes import deserialize, deserialize_by_type, serialize, serialize_by_type
from ..task import Task as TaskItem
from ..task import TaskState
from ..utils import get_utc_datetime
from .base import (
    AgentBase,
    ContextBase,
    EnsembleBase,
    EnsembleMissingError,
    EnsembleNotRunnableError,
    EnsembleStateInconsistentError,
    RecordBase,
    TaskBase,
    kwargs_verify,
)

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


def _taskstate_to_bytes(ts: TaskState) -> bytes:
    return ts.name.lower().encode()


def _taskstate_to_str(ts: TaskState) -> str:
    return ts.name.lower()


class Task(TaskBase):

    def __init__(self, ensemble_identity: str, tr, top_path, retire_path):
        super().__init__(ensemble_identity)

        self._tr = tr
        self._data_path = top_path.create_or_open(self._tr, "task").create_or_open(
            self._tr, ensemble_identity
        )
        self._counter_path = top_path.create_or_open(
            self._tr, "counter"
        ).create_or_open(self._tr, ensemble_identity)
        self._retire_data_path = retire_path.create_or_open(self._tr, "task")
        self._retire_counter_path = retire_path.create_or_open(self._tr, "counter")

    def _get_task_count_by_state(self, state: bytes):
        return (
            deserialize_by_type(self._tr[self._counter_path[state]].value or b"", int)
            or 0
        )

    def num_passed(self) -> int:
        return self._get_task_count_by_state(_taskstate_to_bytes(TaskState.PASSED))

    def num_failed(self) -> int:
        return self._get_task_count_by_state(_taskstate_to_bytes(TaskState.FAILED))

    def num_running(self) -> int:
        return self._get_task_count_by_state(_taskstate_to_bytes(TaskState.RUNNING))

    def num_timedout(self) -> int:
        return self._get_task_count_by_state(_taskstate_to_bytes(TaskState.TIMEDOUT))

    def _new_task(self, task: TaskItem):
        _upsert(
            self._tr,
            self._data_path.create_or_open(
                self._tr, _taskstate_to_str(TaskState.RUNNING)
            ),
            task,
            UpsertType.INSERT,
        )
        _increase_counter(
            self._tr, self._counter_path, _taskstate_to_bytes(TaskState.RUNNING)
        )

    def _set_task_result(
        self,
        identity: str,
        return_value: Optional[int] = None,
        execution_context_identity: Optional[str] = None,
        stdout: Optional[str | bytes] = None,
    ):
        if return_value == 0:
            return_state = TaskState.PASSED
        elif return_value is None:
            return_state = TaskState.TIMEDOUT
        else:
            return_state = TaskState.FAILED
        result = return_state.name.lower().encode()
        _update_counter(self._tr, self._counter_path, result, _FDB_1P)
        _update_counter(
            self._tr,
            self._counter_path,
            TaskState.RUNNING.name.lower().encode(),
            _FDB_1M,
        )

        task_path = self._data_path.open(
            self._tr, _taskstate_to_str(TaskState.RUNNING)
        ).open(self._tr, identity)
        self._tr[task_path[b"state"]] = serialize_by_type(return_state)
        self._tr[task_path[b"execution_context_identity"]] = serialize_by_type(
            execution_context_identity
        )
        self._tr[task_path[b"terminate_time"]] = serialize_by_type(get_utc_datetime())
        self._tr[task_path[b"return_value"]] = serialize_by_type(return_value)
        self._tr[task_path[b"stdout"]] = serialize_by_type(stdout)

        fdb.directory.move(  # type: ignore
            self._tr,
            self._data_path.open(self._tr, _taskstate_to_str(TaskState.RUNNING))
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

    def _get(self, task_identity: str) -> Optional[TaskItem]:
        for key in (state.name.lower() for state in TaskState):
            if (d := self._data_path.create_or_open(self._tr, key)).exists(
                self._tr, task_identity
            ):
                return _get(self._tr, d, task_identity, TaskItem)
        else:
            return None

    def _list_by_taskstate(self, task_state: TaskState):
        p = task_state.name.lower()
        if not self._data_path.exists(self._tr, p):
            return []
        task_path = self._data_path.open(self._tr, p)

        result = []
        for task_identity in task_path.list(self._tr):
            result.append(_get(self._tr, task_path, task_identity, TaskItem))

        return result


class Ensemble(EnsembleBase, _PathMixin):

    @staticmethod
    @fdb.transactional
    def _add_task_transactional(
        tr, ensemble_identity, data_path, top_path, retire_path, args: str
    ) -> str:
        if not data_path.exists(tr, ensemble_identity):
            raise EnsembleMissingError(ensemble_identity)
        ensemble_path = data_path.open(tr, ensemble_identity)
        if tr[ensemble_path[b"state"]] != serialize_by_type(EnsembleState.RUNNABLE):
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
        stdout: Optional[Union[str, bytes]] = None,
    ) -> EnsembleState:
        if not data_path.exists(tr, ensemble_identity):
            raise EnsembleMissingError(ensemble_identity)

        task_obj = Task(ensemble_identity, tr, top_path, retire_path)
        task_obj.set_task_result(
            task_identity, return_value, execution_context_identity, stdout
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
        tr, ensemble_data_path, top_path, retire_path, identity: str
    ) -> Optional[EnsembleItem]:
        result = _get(tr, ensemble_data_path, identity, EnsembleItem)
        if result is None:
            return None

        task_info = Task(identity, tr, top_path, retire_path)
        result.num_failed = task_info.num_failed()
        result.num_passed = task_info.num_passed()
        result.num_running = task_info.num_running()
        result.num_timedout = task_info.num_timedout()

        return result

    @staticmethod
    @fdb.transactional
    def _scan_transactional(
        tr,
        ensemble_data_path,
        top_path,
        retire_path,
        state: Optional[Tuple[EnsembleState]],
        owner: Optional[str] = None,
    ) -> List[EnsembleItem]:
        if state is None:
            serialized_state = tuple()
        else:
            serialized_state = [serialize_by_type(i) for i in state]
        serialized_owner = serialize_by_type(owner)

        result = []
        for ensemble_identity in ensemble_data_path.list(tr):
            ensemble_path = ensemble_data_path.open(tr, ensemble_identity)
            if (
                state is not None
                and tr[ensemble_path[b"state"]].value not in serialized_state
            ):
                continue
            if (
                owner is not None
                and tr[ensemble_path[b"owner"]].value != serialized_owner
            ):
                continue

            ensemble_item = _get(
                tr, ensemble_data_path, ensemble_identity, EnsembleItem
            )
            assert ensemble_item is not None
            task_obj = Task(ensemble_identity, tr, top_path, retire_path)
            ensemble_item.num_passed = task_obj.num_passed()
            ensemble_item.num_failed = task_obj.num_failed()
            ensemble_item.num_running = task_obj.num_running()
            ensemble_item.num_timedout = task_obj.num_timedout()

            result.append(ensemble_item)

        return result

    @staticmethod
    @fdb.transactional
    def _insert_transactional(tr, data_path, counter_path, item: EnsembleItem):
        _upsert(tr, data_path, item, UpsertType.INSERT)
        _increase_counter(tr, counter_path, b"ensemble")

    @staticmethod
    @fdb.transactional
    def _get_task_transactional(
        tr,
        data_path,
        top_path,
        retire_path,
        ensemble_identity: str,
        task_identity: str,
    ):
        if not data_path.exists(tr, ensemble_identity):
            raise EnsembleMissingError(ensemble_identity)
        return Task(ensemble_identity, tr, top_path, retire_path).get(task_identity)

    @staticmethod
    @fdb.transactional
    def _iterate_by_taskstate_transactional(
        tr,
        top_path,
        retire_path,
        ensemble_identity: str,
        task_states: Tuple[TaskState],
    ) -> List[TaskItem]:
        task_obj = Task(ensemble_identity, tr, top_path, retire_path)

        result = []
        for state in task_states:
            for item in task_obj.list_by_taskstate(state):
                result.append(item)

        return result

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
            _db, self._data_path, self._top_path, self._retire_path, identity
        )

    def _retire(self, identity: str):
        return Ensemble._retire_transactional(
            _db, self._data_path, self._retire_path, self._top_path, identity
        )

    def _get_task(
        self, ensemble_identity: str, task_identity: str
    ) -> Optional[TaskItem]:
        return Ensemble._get_task_transactional(
            _db,
            self._data_path,
            self._top_path,
            self._retire_path,
            ensemble_identity,
            task_identity,
        )

    def _iterate_tasks(
        self, ensemble_identity: str, task_state: Optional[TaskState] = None
    ) -> Generator[TaskItem, None, None]:
        if task_state is None:
            task_states = (state for state in TaskState)
        else:
            task_states = (task_state,)

        for item in Ensemble._iterate_by_taskstate_transactional(
            _db, self._top_path, self._retire_path, ensemble_identity, task_states
        ):  # type: ignore
            yield item

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
        return len(list(self._iterate(**kwargs)))

    def _iterate(self, **kwargs):
        kwargs_verify(("state", "owner"), kwargs)
        scan = Ensemble._scan_transactional(
            _db,
            self._data_path,
            self._top_path,
            self._retire_path,
            owner=kwargs.get("owner"),
            state=kwargs.get("state"),
        )

        for item in scan:  # type: ignore
            yield item

    def _add_ensemble_task(self, ensemble_identity: str, args: str | NoneType) -> str:
        return Ensemble._add_task_transactional(
            _db,
            ensemble_identity,
            data_path=self._data_path,
            top_path=self._top_path,
            retire_path=self._retire_path,
            args=args,
        )

    def _set_ensemble_task_result(
        self,
        ensemble_identity: str,
        task_identity: str,
        return_value: int | NoneType,
        execution_context_identity: str | NoneType = None,
        stdout: bytes | str | NoneType = None,
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
            stdout,
        )


class Agent(AgentBase, _PathMixin):

    @staticmethod
    @fdb.transactional
    def _insert_transactional(tr, data_path, counter_path, item: AgentItem):
        _upsert(tr, data_path, item, UpsertType.INSERT)
        _increase_counter(tr, counter_path, "agent")

    @staticmethod
    @fdb.transactional
    def _heartbeat_transactional(tr, data_path, agent_item: AgentItem):
        agent_item.heartbeat = datetime.datetime.now(datetime.timezone.utc)
        _upsert(tr, data_path, agent_item, UpsertType.UPSERT)

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

    def _heartbeat(self, agent_item: AgentItem):
        Agent._heartbeat_transactional(_db, self._data_path, agent_item)
