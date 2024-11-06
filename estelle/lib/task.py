import dataclasses
import datetime
import enum
import uuid

from typing import Optional

TaskState = enum.Enum("TaskState", ["RUNNING", "SUCCEED", "FAILED"])


@dataclasses.dataclass
class Task:
    identity: str
    ensemble_identity: str
    start_time: datetime.datetime
    terminate_time: Optional[datetime.datetime]
    state: TaskState
    args: Optional[str]
    return_value: Optional[int]

    @staticmethod
    def new(ensemble_identity: str, args: Optional[str] = None):
        return Task(
            identity=uuid.uuid4().hex,
            ensemble_identity=ensemble_identity,
            start_time=datetime.datetime.now(datetime.timezone.utc),
            terminate_time=None,
            state=TaskState.RUNNING,
            args=args,
            return_value=None,
        )
