import dataclasses
import pathlib

from typing import Dict, Any, ClassVar, Optional, Protocol, TypeVar, Union

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
from ..serde_bytes import serialize, deserialize
from ..ensemble import Ensemble as EnsembleItem, EnsembleState

fdb.api_version(710)

db = fdb.open()


class DataclassProtocol(Protocol):
    __dataclass_fields__: ClassVar[Dict[str, Any]]


@fdb.transactional
def _upsert(
    tr: fdb.Transaction,
    d: fdb.directory.DirectoryLayer,
    item: DataclassProtocol,
    upsert: bool = True,
) -> bool:
    if not (d.exists(tr) or upsert):
        return False

    assert hasattr(item, "identity")
    identity = getattr(item, "identity")
    row = d.create_or_open(tr, identity)
    for k, v in serialize(item).items():
        row[k] = v


T = TypeVar("T", bound=DataclassProtocol)


@fdb.transactional
def _get[T](
    tr: fdb.Transaction,
    d: fdb.directory.DirectoryLayer,
    identity: Union[str, bytes],
    t: type[T],
) -> Optional[T]:
    if not d.exists(tr, identity):
        return None

    subdir = d.open(tr, identity)
    raw: Dict[bytes, bytes] = {}
    for k in subdir.list():
        raw[k] = subdir[k]

    return deserialize(t, raw)


@fdb.transactional
def _delete(
    tr: fdb.Transaction, d: fdb.directory.DirectoryLayer, identity: Union[str, bytes]
):
    if not d.exists(tr, identity):
        return None

    subdir = d.open(tr, identity)
    raw: Dict[bytes, bytes] = {}
    for k in subdir.list():
        del subdir[k]


class Record(RecordBase):

    def __init__(self, cluster_file: Optional[str | pathlib.Path] = None):
        super().__init__()

        self._top_path = fdb.directory.create_or_open(self._db)

    @property
    def ensemble(self) -> EnsembleBase:
        if self._ensemble is None:
            self._ensemble = Ensemble(self._db, self._top_path)
        return self._ensemble


class Ensemble(EnsembleBase):

    def __init__(self, db, top_path):
        super().__init__()

        self._path = top_path.create_or_open(db)

    def _insert(self, item: EnsembleItem):
        return super()._insert(item)

    def _update_state(
        self,
        identity: str,
        expected_state: Optional[EnsembleState],
        new_state: EnsembleState,
    ):
        pass
