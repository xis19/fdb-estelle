import abc
from types import NoneType
from typing import Generic, Tuple, Optional, TypeVar

from ..agent import Agent
from ..context import Context
from ..ensemble import Ensemble, EnsembleState
from ..task import Task, TaskState

T = TypeVar("T")


class _BaseInterfaceMixin(abc.ABC, Generic[T]):

    def insert(self, item: T):
        """Add a new item to the database, raises KeyError if already inserted"""
        return self._insert(item)

    @abc.abstractmethod
    def _insert(self, item: T):
        raise NotImplementedError()

    def exists(self, identity: str) -> bool:
        """Checks if an item exists with the given identity"""
        return self._exists(identity)

    @abc.abstractmethod
    def _exists(self, identity: str) -> bool:
        raise NotImplementedError()

    def count(self, **kwargs) -> int:
        """Count the number of items fulfilling the criteria"""
        return self._count(**kwargs)

    @abc.abstractmethod
    def _count(self, **kwargs) -> int:
        raise NotImplementedError()

    def get(self, identity: str) -> Optional[T]:
        """Get the item by its identity"""
        return self._get(identity)

    @abc.abstractmethod
    def _get(self, identity: str) -> Optional[T]:
        raise NotImplementedError()

    def iterate(self, **kwargs):
        """Iterate over the table rows"""
        return self._iterate(**kwargs)

    @abc.abstractmethod
    def _iterate(self, **kwargs):
        raise NotImplementedError()


class ContextBase(_BaseInterfaceMixin[Context]):
    """Base class for Context data"""

    @abc.abstractmethod
    def _iterate(self, owner: Optional[str] = None):
        raise NotImplementedError()


class EnsembleStateInconsistentError(RuntimeError):

    def __init__(self, identity: str, expected: EnsembleState, actual: EnsembleState):
        super().__init__(
            f"Unexpected ensemble {identity} state: Expected {expected}, actual: {actual}"
        )
        self._identity = identity
        self._expected = expected
        self._actual = actual

    @property
    def identity(self) -> str:
        return self._identity

    @property
    def expected(self) -> EnsembleState:
        return self._expected

    @property
    def actual(self) -> EnsembleState:
        return self._actual


class EnsembleBase(_BaseInterfaceMixin[Ensemble]):
    """Base class for Ensemble data"""

    @abc.abstractmethod
    def _iterate(self, owner: Optional[str] = None, state: Optional[Tuple[str]] = None):
        raise NotImplementedError()

    @abc.abstractmethod
    def _update_state(
        self,
        identity: str,
        expected_state: EnsembleState | NoneType,
        new_state: EnsembleState,
    ):
        raise NotImplementedError()
    
    def pause(self, identity: str):
        """Pause the ensemble"""
        self._update_state(identity, EnsembleState.RUNNABLE, EnsembleState.STOPPED)

    def resume(self, identity: str):
        """Resume the ensemble"""
        self._update_state(identity, EnsembleState.STOPPED, EnsembleState.RUNNABLE)

    def kill(self, identity: str):
        """Kill the ensemble"""
        self._update_state(identity, None, EnsembleState.KILLED)
    
    @abc.abstractmethod
    def _try_update_final_state(self, identity: str):
        raise NotImplementedError()
    
    def try_update_final_state(self, identity: str):
        """Check if the ensemble should be marked as COMPLETED or FAILED"""
        self._try_update_final_state(identity)
    

class TaskBase(_BaseInterfaceMixin[Task]):
    """Base class for Task data"""

    @abc.abstractmethod
    def _iterate(self):
        raise NotImplementedError()


class AgentBase(_BaseInterfaceMixin[Agent]):
    """Base class for Agents"""

    @abc.abstractmethod
    def _iterate(self):
        raise NotImplementedError()

    def heartbeat(self, agent: Agent):
        return self._heartbeat(agent)

    @abc.abstractmethod
    def _heartbeat(self, agent: Agent):
        raise NotImplementedError()


class EnsembleMissingError(RuntimeError):

    def __init__(self, identity: str):
        super().__init__(f"Missing ensemble {identity}")

        self._identity = identity

    @property
    def identity(self) -> str:
        return self._identity


class EnsembleNotRunnableError(EnsembleStateInconsistentError):

    def __init__(self, identity: str, state: EnsembleState):
        super().__init__(identity, EnsembleState.RUNNABLE, state)


class EnsembleTaskBase(abc.ABC):
    """Combined Ensemble and Task transactions"""

    def report_start_task(self, task: Task):
        """Report a task started"""
        self._report_start_task(task)

    def report_task_result(self, task_identity: str, return_value: Optional[int]):
        """Report task result for a given ensemble"""
        self._report_task_result(task_identity, return_value)

    @abc.abstractmethod
    def _report_start_task(self, task: Task):
        raise NotImplementedError()

    @abc.abstractmethod
    def _report_task_result(self, task_identity: str, return_value: Optional[int]):
        raise NotImplementedError()


class RecordBase(abc.ABC):

    def __init__(self):
        self._ensemble: Optional[EnsembleBase] = None
        self._task: Optional[TaskBase] = None
        self._context: Optional[ContextBase] = None
        self._agent: Optional[AgentBase] = None

        self._ensemble_task: Optional[EnsembleTaskBase] = None

    @property
    @abc.abstractmethod
    def ensemble(self) -> EnsembleBase:
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def task(self) -> TaskBase:
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def context(self) -> ContextBase:
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def agent(self) -> AgentBase:
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def ensemble_task(self) -> EnsembleTaskBase:
        raise NotImplementedError()
