import dataclasses
import datetime
import enum
import uuid

from typing import Optional

EnsembleState = enum.Enum(
    "EnsembleState",
    ["RUNNABLE", "STOPPED", "TIMEDOUT", "FAILED", "COMPLETED", "KILLED"],
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
