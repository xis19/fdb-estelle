from sqlalchemy import (
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
)
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import registry

from ..agent import Agent
from ..context import Context
from ..ensemble import Ensemble, EnsembleState
from ..task import Task, TaskState

_mapper_registry = registry()
metadata = MetaData()

context_table = Table(
    "context",
    metadata,
    Column("identity", String, primary_key=True),
    Column("owner", String),
    Column("size", Integer, nullable=False),
    Column("checksum", String),
    Column("tag", String, nullable=True),
)
_mapper_registry.map_imperatively(
    Context,
    context_table,
)

ensemble_table = Table(
    "ensemble",
    metadata,
    Column("identity", String, primary_key=True),
    Column("owner", String, nullable=False),
    Column("priority", Integer, nullable=False, default=0),
    Column("create_time", DateTime, nullable=False),
    Column("start_time", DateTime, nullable=True),
    Column("terminate_time", DateTime, nullable=True),
    Column("state", Enum(EnsembleState), nullable=False),
    Column("state_last_modified_time", DateTime),
    Column("total_runs", Integer, nullable=False),
    Column("num_passed", Integer, nullable=False),
    Column("num_failed", Integer, nullable=False),
    Column(
        "context_identity", String, ForeignKey(context_table.c.identity), nullable=False
    ),
    Column("executable", String, nullable=False),
    Column("timeout", Integer, nullable=True),
    Column("time_used", Integer, nullable=False),
    Column("max_fails", Integer, nullable=True),
)
_mapper_registry.map_imperatively(Ensemble, ensemble_table)

task_table = Table(
    "task",
    metadata,
    Column("identity", String, primary_key=True),
    Column(
        "ensemble_identity",
        String,
        ForeignKey(ensemble_table.c.identity),
        nullable=False,
    ),
    Column("start_time", DateTime, nullable=True),
    Column("terminate_time", DateTime, nullable=True),
    Column("state", Enum(TaskState), nullable=False),
    Column("args", String, nullable=True),
    Column("return_value", Integer, nullable=True),
)
_mapper_registry.map_imperatively(Task, task_table)

agent_table = Table(
    "agent",
    metadata,
    Column("identity", String, primary_key=True),
    Column("hostname", String),
    Column("start_time", DateTime, nullable=False),
    Column("heartbeat", DateTime, nullable=True),
    Column("tasks_succeed", Integer),
    Column("tasks_failed", Integer),
)
_mapper_registry.map_imperatively(Agent, agent_table)
