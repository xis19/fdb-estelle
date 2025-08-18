import dataclasses
import datetime
import enum
import socket
import uuid
from typing import Optional

from ..config import config
from ..utils import get_utc_datetime


class TaskExecuteStage(enum.IntEnum):
    CONSTRUCT = 1
    SETUP = 2
    EXECUTE = 3
    TEARDOWN = 4
    IDLE = 5


@dataclasses.dataclass
class Agent:
    identity: str
    hostname: str
    start_time: datetime.datetime = datetime.datetime.now(datetime.timezone.utc)
    heartbeat: Optional[datetime.datetime] = None
    tasks_succeed: int = 0
    tasks_failed: int = 0
    current_ensemble: Optional[str] = None
    current_task: Optional[str] = None
    current_task_stage: TaskExecuteStage = TaskExecuteStage.IDLE

    @staticmethod
    def new() -> "Agent":
        return Agent(identity=uuid.uuid4().hex, hostname=socket.gethostname())


def is_stale(agent: Agent) -> bool:
    if agent.heartbeat is None:
        return True

    dt = (get_utc_datetime() - agent.heartbeat).seconds
    if dt > config.agent.max_idle_time:
        return True

    return False
