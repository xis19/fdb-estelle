import dataclasses
import datetime
import enum
import uuid

from typing import Optional

from .utils import get_utc_datetime


class EnsembleState(enum.IntEnum):
    RUNNABLE = 0
    STOPPED = 1
    TIMEDOUT = 2
    FAILED = 3
    COMPLETED = 4
    KILLED = 5

    @staticmethod
    def is_terminated(state: "EnsembleState") -> bool:
        return state in (
            EnsembleState.COMPLETED,
            EnsembleState.FAILED,
            EnsembleState.KILLED,
        )


@dataclasses.dataclass
class Ensemble:
    identity: str
    owner: str
    priority: int
    create_time: datetime.datetime
    start_time: Optional[datetime.datetime]
    terminate_time: Optional[datetime.datetime]
    state: EnsembleState
    state_last_modified_time: datetime.datetime
    total_runs: int
    num_passed: int
    num_failed: int
    context_identity: str
    executable: str
    timeout: Optional[int]
    time_used: int
    max_fails: Optional[int]

    @staticmethod
    def new(
        owner: str,
        priority: int,
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
            priority=priority,
            create_time=_now,
            start_time=None,
            terminate_time=None,
            state=EnsembleState.RUNNABLE,
            state_last_modified_time=_now,
            total_runs=total_runs,
            num_passed=0,
            num_failed=0,
            context_identity=context_identity,
            executable=executable,
            timeout=timeout,
            time_used=0,
            max_fails=max_fails,
        )

    def is_failed(self) -> bool:
        return self.max_fails is not None and self.num_failed >= self.max_fails

    def is_completed(self) -> bool:
        return not self.is_failed() and (
            self.num_passed + self.num_failed >= self.total_runs
        )

    def updated_time_used(self, now: Optional[datetime.datetime] = None):
        if self.start_time is not None:
            if now is None:
                now = get_utc_datetime()
            self.time_used += (now - self.start_time).seconds
