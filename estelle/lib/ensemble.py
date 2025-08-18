import dataclasses
import datetime
import enum
import uuid
from typing import Optional

from .utils import get_utc_datetime


class EnsembleState(enum.IntEnum):
    RUNNABLE = 1
    STOPPED = 2
    TIMEDOUT = 3
    FAILED = 4
    COMPLETED = 5
    KILLED = 6

    @staticmethod
    def is_terminated(state: "EnsembleState") -> bool:
        return state in (
            EnsembleState.COMPLETED,
            EnsembleState.TIMEDOUT,
            EnsembleState.FAILED,
            EnsembleState.KILLED,
        )


@dataclasses.dataclass
class Ensemble:
    identity: str
    owner: str
    create_time: datetime.datetime
    start_time: Optional[datetime.datetime]
    terminate_time: Optional[datetime.datetime]
    state: EnsembleState
    state_last_modified_time: datetime.datetime
    total_runs: int
    context_identity: str
    executable: str
    timeout: Optional[int]
    time_used: int
    max_fails: Optional[int]
    num_running: int = 0
    num_passed: int = 0
    num_failed: int = 0
    num_timedout: int = 0

    @staticmethod
    def new(
        owner: str,
        total_runs: int,
        context_identity: str,
        executable: str,
        timeout: Optional[int],
        max_fails: Optional[int],
    ) -> "Ensemble":
        _now = datetime.datetime.now(datetime.timezone.utc)
        return Ensemble(
            identity=uuid.uuid4().hex,
            owner=owner,
            create_time=_now,
            start_time=None,
            terminate_time=None,
            state=EnsembleState.RUNNABLE,
            state_last_modified_time=_now,
            total_runs=total_runs,
            context_identity=context_identity,
            executable=executable,
            timeout=timeout,
            time_used=0,
            max_fails=max_fails,
        )

    def is_timed_out(self) -> bool:
        return self.timeout is not None and self.time_used > self.timeout

    def updated_time_used(self, now: Optional[datetime.datetime] = None):
        if self.start_time is not None:
            if now is None:
                now = get_utc_datetime()
            self.time_used += (now - self.start_time).seconds
