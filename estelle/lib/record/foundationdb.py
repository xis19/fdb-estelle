import enum

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
    EnsembleTaskBase,
    RecordBase,
    TaskBase,
)
from ..serde_bytes import serialize, deserialize, serialize_by_type, deserialize_by_type
from ..ensemble import Ensemble as EnsembleItem, EnsembleState
from ..context import Context as ContextItem
from ..agent import Agent as AgentItem
from ..task import Task as TaskItem, TaskState
from ..utils import get_utc_datetime

fdb.api_version(710)

_db = fdb.open()


class DataclassProtocol(Protocol):
    __dataclass_fields__: ClassVar[Dict[str, Any]]
    identity: str


class UpsertType(enum.IntEnum):
    INSERT = 0
    UPSERT = 1
    UPDATE = 2


@fdb.transactional
def _upsert(
    tr: fdb.Transaction,
    d: fdb.DirectoryLayer,
    item: DataclassProtocol,
    upsert: UpsertType = UpsertType.UPDATE,
) -> bool:
    if not d.exists(tr, item.identity) and upsert is UpsertType.UPDATE:
        logger.warning("Not inserting since upsert flag is set to be UPDATE")
        return False

    if d.exists(tr, item.identity) and upsert is UpsertType.INSERT:
        logger.warning("Not updating since upsert flag is set to be INSERT")
        return False

    assert hasattr(item, "identity")
    identity = getattr(item, "identity")
    subdir = d.create_or_open(tr, identity)
    for k, v in serialize(item).items():
        logger.debug(f"Writing {subdir[k]} -- {v}")
        tr[subdir[k]] = v

    return True


_T = TypeVar("_T", bound=DataclassProtocol)


def _get_impl[_T](tr: fdb.Transaction, d: fdb.DirectoryLayer, t: type[_T]) -> _T:
    raw = {}
    for k, v in tr.get_range_startswith(d):
        _, key = fdb.tuple.unpack(k)
        raw[key] = v

    return deserialize(t, raw)


@fdb.transactional
def _get[_T](
    tr: fdb.Transaction,
    d: fdb.DirectoryLayer,
    identity: Union[str, bytes],
    t: type[_T],
) -> Optional[_T]:
    if not d.exists(tr, identity):
        return None

    return _get_impl(tr, d.open(tr, identity), t)


@fdb.transactional
def _delete(tr: fdb.Transaction, d: fdb.DirectoryLayer, identity: Union[str, bytes]):
    d.remove_if_exists(tr, identity)


@fdb.transactional
def _scan[_T](tr: fdb.Transaction, d: fdb.DirectoryLayer, t: type[_T]) -> List[_T]:
    result: List[_T] = []
    for key in d.list(tr):
        result.append(_get(tr, d, key, t))

    return result


def _filter[_T](
    tr: fdb.Transaction,
    d: fdb.DirectoryLayer,
    t: type[_T],
    conditioner: Callable[[_T], bool],
) -> List[_T]:
    return [item for item in _scan(tr, d, t) if conditioner(item)]


def _count[_T](
    tr: fdb.Transaction,
    d: fdb.DirectoryLayer,
    t: type[_T],
    conditioner: Callable[[_T], bool],
) -> int:
    return len(_filter(tr, d, t, conditioner))


class Record(RecordBase):

    def __init__(self):
        super().__init__()

        self._top_path = fdb.directory.create_or_open(_db, "estelle")

    @property
    def top_path(self):
        return self._top_path

    @property
    def ensemble(self) -> EnsembleBase:
        if self._ensemble is None:
            self._ensemble = Ensemble(self._top_path)
        return self._ensemble

    @property
    def context(self) -> ContextBase:
        if self._context is None:
            self._context = Context(self._top_path)
        return self._context

    @property
    def task(self) -> TaskBase:
        if self._task is None:
            self._task = Task(self._top_path)
        return self._task

    @property
    def agent(self) -> AgentBase:
        if self._agent is None:
            self._agent = Agent(self._top_path)
        return self._agent

    @property
    def ensemble_task(self) -> EnsembleTaskBase:
        return None


class Context(ContextBase):
    @staticmethod
    def _filter_func(c: ContextItem, owner: Optional[str] = None):
        return owner is None or c.owner == owner

    def __init__(self, top_path):
        super().__init__()

        self._path = top_path.create_or_open(_db, "context")

    def _insert(self, item: ContextItem):
        _upsert(_db, self._path, item, UpsertType.INSERT)

    def _get(self, identity: str) -> Optional[ContextItem]:
        return _get(_db, self._path, identity, ContextItem)

    def _count(self) -> int:
        return len(_scan(_db, self._path, ContextItem))

    def _iterate(self, owner: Optional[str] = None):
        for item in _filter(
            _db, self._path, ContextItem, lambda t: Context._filter_func(t, owner)
        ):
            yield item

    def _exists(self, identity: str) -> bool:
        return self._get(identity) is not None


class Ensemble(EnsembleBase):

    @staticmethod
    @fdb.transactional
    def _update_state_impl(
        tr: fdb.Transaction,
        path: fdb.DirectoryLayer,
        identity: str,
        expected_state: Optional[Union[EnsembleState, Sequence[EnsembleState]]],
        new_state: EnsembleState,
    ):
        if not path.exists(tr, identity):
            raise EnsembleMissingError(identity)
        subpath = path.open(tr, identity)
        ensemble: EnsembleItem = _get_impl(tr, subpath, EnsembleItem)
        Ensemble._get_updated_state(ensemble, expected_state, new_state)
        tr[subpath["time_used"]] = serialize_by_type(ensemble.time_used)
        tr[subpath["start_time"]] = serialize_by_type(ensemble.start_time)
        tr[subpath["state"]] = serialize_by_type(ensemble.state)
        tr[subpath["state_last_modified_time"]] = serialize_by_type(
            ensemble.state_last_modified_time
        )

    @staticmethod
    @fdb.transactional
    def _try_update_final_state_impl(
        tr: fdb.Transaction,
        path: fdb.DirectoryLayer,
        identity: str,
    ):
        if not path.exists(tr, identity):
            raise EnsembleMissingError(identity)
        ensemble: EnsembleItem = _get_impl(tr, path.open(tr, identity), EnsembleItem)

        if Ensemble._get_final_state(ensemble):
            tr[path["state"]] = serialize_by_type(ensemble.state)
            tr[path["terminate_time"]] = serialize_by_type(ensemble.terminate_time)
            tr[path["time_used"]] = serialize_by_type(ensemble.time_used)

    @staticmethod
    def _filter_func(
        e: EnsembleItem,
        state: Optional[EnsembleState] = None,
        owner: Optional[str] = None,
    ) -> bool:
        return (state is None or e.state is state) and (
            owner is None or e.owner == owner
        )

    def __init__(self, top_path):
        super().__init__()

        self._path = top_path.create_or_open(_db, "ensemble")

    @property
    def path(self):
        return self._path

    def _insert(self, item: EnsembleItem):
        _upsert(self._path, item, UpsertType.INSERT)

    def _exists(self, identity: str) -> bool:
        return self._get(identity) is not None

    def _get(self, identity: str) -> Optional[EnsembleItem]:
        return _get(self._path, identity, EnsembleItem)

    def _count(
        self,
        owner: Optional[str] = None,
        state: Optional[EnsembleState] = None,
    ) -> int:
        return _count(
            _db,
            self._path,
            EnsembleItem,
            lambda t: Ensemble._filter_func(t, state, owner),
        )

    def _iterate(
        self, owner: Optional[str] = None, state: Optional[EnsembleState] = None
    ):
        for item in _filter(
            _db,
            self._path,
            EnsembleItem,
            lambda t: Ensemble._filter_func(t, state, owner),
        ):
            yield item

    def _update_state(
        self,
        identity: str,
        expected_state: Optional[EnsembleState],
        new_state: EnsembleState,
    ):
        return Ensemble._update_state_impl(
            _db, self._path, identity, expected_state, new_state
        )

    def _try_update_final_state(self, identity: str):
        return Ensemble._try_update_final_state_impl(_db, self._path, identity)


class Task(TaskBase):

    @staticmethod
    def _filter_func(item: TaskItem, ensemble_identity: Optional[str] = None) -> bool:
        return ensemble_identity is None or item.ensemble_identity == ensemble_identity

    def __init__(self, top_path):
        super().__init__()

        self._path = top_path.create_or_open(_db, "task")

    @property
    def path(self):
        return self._path

    def _insert(self, item: TaskItem):
        _upsert(self._path, item, UpsertType.INSERT)

    def _exists(self, identity: str) -> bool:
        return self._get(identity) is not None

    def _get(self, identity: str) -> Optional[TaskItem]:
        return _get(self._path, identity, TaskItem)

    def _count(self, ensemble_identity: Optional[str] = None) -> int:
        return _count(
            _db, self._path, TaskItem, lambda t: Task._filter_func(t, ensemble_identity)
        )

    def _iterate(self):
        for item in _scan(_db, self._path, TaskItem):
            yield item


class Agent(AgentBase):

    def __init__(self, top_path):
        super().__init__()

        self._path = top_path.create_or_open(_db, "agent")

    @property
    def path(self):
        return self._path

    def _insert(self, item: AgentItem):
        _upsert(self._path, item, UpsertType.INSERT)

    def _exists(self, identity: str) -> bool:
        return self._get(identity) is not None

    def _get(self, identity: str) -> Optional[AgentItem]:
        return _get(self._path, identity, AgentItem)

    def _count(self) -> int:
        return len(_scan(_db, self._path, AgentItem))

    def _iterate(self):
        for item in _scan(_db, self._path, AgentItem):
            yield item

    def _heartbeat(self, agent: AgentItem):
        _upsert(_db, self._path, agent, UpsertType.UPSERT)


class EnsembleTask(EnsembleTaskBase):

    def __init__(self, ensemble: Ensemble, task: Task):
        super().__init__()

        self._ensemble_ = ensemble
        self._task_ = task

    @staticmethod
    @fdb.transactional
    def _report_start_task_impl(tr: fdb.Transaction, record: Record, task: TaskItem):
        record.ensemble.path

    @staticmethod
    @fdb.transactional
    def _report_task_result_impl(task_identity: str, return_value: Optional[int]):
        pass

    def _report_start_task(self, task: TaskItem):
        EnsembleTask._report_start_task_impl(task)

    def _report_task_result(
        self, task_identity: str, return_value: Optional[int] = None
    ):
        EnsembleTask._report_task_result_impl(task_identity, return_value)
