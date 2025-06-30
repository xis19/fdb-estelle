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


@fdb.transactional
def _upsert(
    tr: fdb.Transaction,
    d: fdb.DirectoryLayer,
    item: DataclassProtocol,
    upsert: bool = True,
) -> bool:
    if not (d.exists(tr) or upsert):
        logger.warning("Not inserting since upsert flag is set to be False")
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

        self._top_path = fdb.directory.create_or_open(_db)

    @property
    def ensemble(self) -> EnsembleBase:
        if self._ensemble is None:
            self._ensemble = Ensemble(self._top_path)
        return self._ensemble


class Context(ContextBase):
    @staticmethod
    def _filter_func(c: ContextItem, owner: Optional[str] = None):
        return owner is None or c.owner == owner

    def __init__(self, top_path):
        super().__init__()

        self._path = top_path.create_or_open(_db)

    def _iterate(self, owner: Optional[str] = None):
        for item in _filter(
            _db, self._path, ContextItem, lambda t: Context._filter_func(t, owner)
        ):
            yield item


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

    def _insert(self, item: EnsembleItem):
        _upsert(self._path, item, False)

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

    def _insert(self, item: TaskItem):
        _upsert(self._path, item, False)

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

    def _insert(self, item: AgentItem):
        _upsert(self._path, item, False)

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
        _upsert()
