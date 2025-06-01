import pathlib

from typing import Optional

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
from ..ensemble import Ensemble as EnsembleItem, EnsembleState


class Record(RecordBase):

    def __init__(self, cluster_file: Optional[str | pathlib.Path] = None):
        super().__init__()

        fdb.api_version(740)
        self._db = fdb.open(cluster_file)
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
