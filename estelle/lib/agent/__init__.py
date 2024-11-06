import dataclasses
import datetime
import uuid
import socket

from typing import Optional

from ..config import config
from ..utils import get_utc_datetime


@dataclasses.dataclass
class Agent:
    identity: str
    hostname: str
    start_time: datetime.datetime = datetime.datetime.now(datetime.timezone.utc)
    heartbeat: Optional[datetime.datetime] = None
    tasks_succeed: int = 0
    tasks_failed: int = 0

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
