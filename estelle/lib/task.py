import dataclasses
import datetime
import enum
import uuid
from typing import Optional


class TaskState(enum.IntEnum):
    RUNNING = 1
    PASSED = 2
    FAILED = 3
    TIMEDOUT = 4


@dataclasses.dataclass
class Task:
    identity: str
    ensemble_identity: str
    start_time: datetime.datetime
    terminate_time: Optional[datetime.datetime]
    state: TaskState
    args: str
    return_value: Optional[int]
    execution_context_identity: Optional[str]
    stdout: str = ""

    @staticmethod
    def new(ensemble_identity: str, args: str) -> "Task":
        return Task(
            identity=uuid.uuid4().hex,
            ensemble_identity=ensemble_identity,
            start_time=datetime.datetime.now(datetime.timezone.utc),
            terminate_time=None,
            state=TaskState.RUNNING,
            args=args,
            return_value=None,
            execution_context_identity=None,
        )
