from copy import deepcopy
from datetime import datetime, timezone
from dataclasses import asdict
from sqlite3 import Connection as SQLite3Connection
from typing import Mapping, Optional, Sequence

import sqlalchemy
import sqlalchemy_utils

from loguru import logger
from sqlalchemy import Engine, event, func, select, Select
from sqlalchemy.orm import Session, sessionmaker

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
from ._sql_base import agent_table, context_table, ensemble_table, metadata, task_table
from ..agent import Agent as AgentItem
from ..config import config
from ..context import Context as ContextItem
from ..ensemble import Ensemble as EnsembleItem, EnsembleState
from ..task import Task as TaskItem, TaskState
from ..utils import get_utc_datetime


_IDENTITY_SHORTCUT_LENGTH: int = 8


def _verify_identity_length(identity: str):
    if len(identity) < _IDENTITY_SHORTCUT_LENGTH:
        raise KeyError(
            f"Identity too short: {identity} ({len(identity)} < {_IDENTITY_SHORTCUT_LENGTH})"
        )


@event.listens_for(Engine, "connect")
def _set_sqlite_pragma(dbapi_connection, connection_record):
    """For SQLite3, it is required to execute certain PRAGMA to enable foreign keys restriction"""
    logger.info("Turning foreign keys on for SQLite3")
    if isinstance(dbapi_connection, SQLite3Connection):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON;")
        cursor.close()


class _DBServer:

    def __init__(self, connection_string: str):
        self._connection_string: str = connection_string
        self._engine: Optional[Engine] = None
        self._session: Optional[Session] = None

    @property
    def connection_string(self) -> str:
        return self._connection_string

    @property
    def engine(self) -> Engine:
        if self._engine is None:
            self._engine = sqlalchemy.create_engine(self.connection_string)

            if not sqlalchemy_utils.database_exists(self._engine.url):
                sqlalchemy_utils.create_database(self._engine.url)
                metadata.create_all(self._engine)

            self._session = sessionmaker(self._engine)

        return self._engine

    @property
    def session(self) -> Session:
        if self._session is None:
            self.engine
        return self._session


class Record(RecordBase):

    def __init__(self, connection_string: Optional[str] = None):
        super().__init__()

        connection_string = connection_string or config.record.sql_connect_string
        self._db = _DBServer(connection_string)

    @property
    def ensemble(self) -> EnsembleBase:
        if self._ensemble is None:
            self._ensemble = Ensemble(self._db)
        return self._ensemble

    @property
    def context(self) -> ContextBase:
        if self._context is None:
            self._context = Context(self._db)
        return self._context

    @property
    def task(self) -> TaskBase:
        if self._task is None:
            self._task = Task(self._db)
        return self._task

    @property
    def agent(self) -> AgentBase:
        if self._agent is None:
            self._agent = Agent(self._db)
        return self._agent

    @property
    def ensemble_task(self) -> EnsembleTaskBase:
        if self._ensemble_task is None:
            self._ensemble_task = EnsembleTask(self._db)
        return self._ensemble_task


def _select_constructor(table, where=None, columns=None) -> Select:
    if isinstance(columns, Sequence):
        statement = select(*tuple(columns))
    elif columns is None:
        statement = select(table)
    else:
        statement = select(columns)

    statement = statement.select_from(table)

    if isinstance(where, Sequence):
        for where_item in where:
            statement = statement.where(where_item)
    elif where is not None:
        statement = statement.where(where)

    return statement


def _select_count_constructor(table, where=None) -> Select:
    return _select_constructor(table, where=where, columns=func.count("*"))


def _select_identity_count_constructor(table, identity: str) -> Select:
    _verify_identity_length(identity)
    return _select_count_constructor(table, table.c.identity.startswith(identity))


def _exists_impl(self, table, identity) -> bool:
    query = _select_identity_count_constructor(table, identity)
    with self._db.session() as session:
        return session.scalars(query).one() == 1


def _get_impl(self, table, identity):
    with self._db.session() as session:
        ret = session.query(table).filter(table.c.identity == identity).one_or_none()
        return ret


def _insert_impl(self, item):
    with self._db.session() as session:
        session.add(deepcopy(item))
        session.commit()


def _query_yielder_impl(self, query):
    with self._db.session() as session:
        for item in session.execute(query).yield_per(10):
            yield item


class Context(ContextBase):

    def __init__(self, db: _DBServer):
        self._db = db

    _insert = _insert_impl

    _exists = lambda self, identity: _exists_impl(self, context_table, identity)

    _query_yielder = _query_yielder_impl

    def _count(self, owner: Optional[str] = None) -> int:
        query = _select_count_constructor(context_table, context_table.owner == owner)
        with self._db.session() as session:
            return session.scalars(query).one()

    def _iterate(self, owner: Optional[str]):
        wheres = []
        if owner is not None:
            wheres.append(context_table.owner == owner)
        query = _select_constructor(context_table, where=wheres)
        return self._query_yielder(query)

    def _get(self, identity: str) -> Optional[ContextItem]:
        result = _get_impl(self, context_table, identity)
        if result is None:
            return None
        return ContextItem(*result)


class Ensemble(EnsembleBase):

    def __init__(self, db: _DBServer):
        self._db = db

    _insert = _insert_impl

    _exists = lambda self, identity: _exists_impl(self, ensemble_table, identity)

    _query_yielder = _query_yielder_impl

    def _count(
        self, state: Optional[EnsembleState] = None, owner: Optional[str] = None
    ) -> int:
        wheres = []
        if state is not None:
            wheres.append(ensemble_table.c.state.in_(tuple(state)))
        if owner is not None:
            wheres.append(ensemble_table.c.owner == owner)
        query = _select_count_constructor(ensemble_table, where=wheres)
        with self._db.session() as session:
            return session.scalars(query).one()

    def _iterate(self, owner: Optional[str], state: Optional[Sequence[str]]):
        wheres = []
        if state is not None:
            wheres.append(ensemble_table.c.state.in_(tuple(state)))
        if owner is not None:
            wheres.append(ensemble_table.c.owner == owner)
        query = _select_constructor(ensemble_table, where=wheres)
        return self._query_yielder(query)

    _get = lambda self, identity: EnsembleItem(
        *_get_impl(self, ensemble_table, identity)
    )

    def _try_update_final_state(self, identity: str):
        check_time = get_utc_datetime()
        with self._db.session() as session:
            ensemble: Optional[EnsembleItem] = (
                session.query(ensemble_table)
                .filter(ensemble_table.c.identity == identity)
                .one_or_none()
            )
            if ensemble is None:
                raise EnsembleMissingError(identity)

            if ensemble.state in (
                EnsembleState.COMPLETED,
                EnsembleState.FAILED,
                EnsembleState.KILLED,
            ):
                return

            possible_new_time_used = (
                ensemble.time_used + (check_time - ensemble.start_time).seconds
            )
            update_fields = None
            if ensemble.num_failed >= ensemble.max_fails:
                update_fields = {ensemble_table.c.state: EnsembleState.FAILED}
            elif ensemble.num_passed + ensemble.num_failed >= ensemble.total_runs:
                update_fields = {ensemble_table.c.state: EnsembleState.COMPLETED}

            if update_fields is not None:
                update_fields.update({ensemble_table.c.terminate_time: check_time})
                if ensemble.state is EnsembleState.RUNNABLE:
                    update_fields.update(
                        {ensemble_table.c.time_used: possible_new_time_used}
                    )

                session.query(ensemble_table).filter(
                    ensemble_table.c.identity == identity
                ).update(update_fields)
            
            session.commit()

    def _update_state(
        self,
        identity: str,
        expected_state: Optional[EnsembleState | Sequence[EnsembleState]],
        new_state: EnsembleState,
    ):
        now = get_utc_datetime()
        update_fields = {
            ensemble_table.c.state: new_state,
            ensemble_table.c.state_last_modified_time: now,
        }
        if isinstance(expected_state, EnsembleState):
            expected_state = (expected_state,)

        with self._db.session() as session:
            ensemble: Optional[EnsembleItem] = (
                session.query(ensemble_table)
                .filter(ensemble_table.c.identity.startswith(identity))
                .one_or_none()
            )
            if ensemble is None:
                raise EnsembleMissingError(identity)
            if expected_state is not None and ensemble.state is not expected_state:
                raise EnsembleStateInconsistentError(
                    identity, expected_state, ensemble.state
                )

            if (
                ensemble.state is EnsembleState.RUNNABLE
                and new_state is not EnsembleState.RUNNABLE
                and ensemble.start_time is not None
            ):
                update_fields[ensemble_table.c.time_used] = (
                    ensemble.time_used + (now - ensemble.start_time).seconds
                )
            if (
                ensemble.state is not EnsembleState.RUNNABLE
                and new_state is EnsembleState.RUNNABLE
            ):
                update_fields[ensemble_table.c.start_time] = now

            session.query(ensemble_table).filter(
                ensemble_table.c.identity.startswith(identity)
            ).update(update_fields)

            session.commit()


class Task(TaskBase):

    def __init__(self, db: _DBServer):
        self._db = db

    _insert = _insert_impl

    _exists = lambda self, identity: _exists_impl(self, task_table, identity)

    _query_yielder = _query_yielder_impl

    def _count(self, ensemble_identity: Optional[str] = None) -> int:
        wheres = []
        if ensemble_identity is not None:
            wheres.append(task_table.ensemble_identity == ensemble_identity)
        query = _select_count_constructor(ensemble_table, where=wheres)
        with self._db.session() as session:
            return session.scalars(query).one()

    def _iterate(self):
        query = _select_constructor(task_table)
        return self._query_yielder(query)

    _get = lambda self, identity: TaskItem(*_get_impl(self, task_table, identity))


class Agent(AgentBase):
    def __init__(self, db: _DBServer):
        self._db = db

    _insert = _insert_impl

    _exists = lambda self, identity: _exists_impl(self, agent_table, identity)

    _query_yielder = _query_yielder_impl

    def _count(self) -> int:
        query = _select_count_constructor(agent_table)
        with self._db.session() as session:
            return session.scalars(query).one()

    def _iterate(self):
        query = _select_constructor(agent_table)
        return self._query_yielder(query)

    def _heartbeat(self, agent: AgentItem):
        with self._db.session() as session:
            session.query(agent_table).filter(
                agent_table.c.identity == agent.identity
            ).update(asdict(agent))

            session.commit()

    _get = lambda self, identity: AgentItem(*_get_impl(self, agent_table, identity))


class EnsembleTask(EnsembleTaskBase):

    def __init__(self, db: _DBServer):
        self._db = db

    def _get_ensemble(self, identity: str, session: Session) -> EnsembleItem:
        ensemble: Optional[EnsembleItem] = (
            session.query(ensemble_table)
            .filter(ensemble_table.c.identity == identity)
            .one_or_none()
        )
        if ensemble is None:
            raise EnsembleMissingError(identity)
        return ensemble

    def _update_ensemble(self, identity: str, update_map: Mapping, session: Session):
        session.query(ensemble_table).filter(
            ensemble_table.c.identity == identity
        ).update(update_map)

    def _report_start_task(self, task: TaskItem):
        with self._db.session() as session:
            ensemble: EnsembleItem = self._get_ensemble(task.ensemble_identity, session)
            if (
                ensemble.state != EnsembleState.RUNNABLE
                or ensemble.max_fails <= ensemble.num_failed
            ):
                raise EnsembleNotRunnableError(task.ensemble_identity, ensemble.state)

            session.add(deepcopy(task))

            ensemble_update_map = {
                ensemble_table.c.state_last_modified_time: task.start_time
            }
            if ensemble.start_time is None:
                ensemble_update_map[ensemble_table.c.start_time] = task.start_time
            self._update_ensemble(ensemble.identity, ensemble_update_map, session)

            session.commit()

    def _report_task_result(self, identity: str, return_value: Optional[int]):
        if return_value == 0:
            state = TaskState.SUCCEED
        else:
            state = TaskState.FAILED

        with self._db.session() as session:
            task: Optional[TaskItem] = (
                session.query(task_table)
                .filter(task_table.c.identity == identity)
                .one_or_none()
            )
            if task is None:
                raise KeyError(f"Task identity {identity} is not valid")

            ensemble = self._get_ensemble(task.ensemble_identity, session)

            task_terminate_time = datetime.now(timezone.utc)
            session.query(task_table).filter(task_table.c.identity == identity).update(
                {
                    task_table.c.state: state,
                    task_table.c.terminate_time: task_terminate_time,
                    task_table.c.return_value: return_value,
                }
            )

            ensemble_update_map = {
                ensemble_table.c.state_last_modified_time: task_terminate_time
            }
            if state == TaskState.SUCCEED:
                ensemble_update_map[ensemble_table.c.num_passed] = (
                    ensemble.num_passed + 1
                )
            else:
                ensemble_update_map[ensemble_table.c.num_failed] = (
                    ensemble.num_failed + 1
                )

            self._update_ensemble(ensemble.identity, ensemble_update_map, session)

            session.commit()
