import abc
from types import NoneType
from typing import Any, Generator, Generic, Mapping, Optional, Sequence, TypeVar, Union

from ..agent import Agent
from ..context import Context
from ..ensemble import Ensemble, EnsembleState
from ..task import Task, TaskState
from ..utils import get_utc_datetime

T = TypeVar("T")


def kwargs_verify(items: Sequence[str], kwarg: Mapping[str, Any]):
    supported = set(items)
    keys = set(kwarg.keys())
    if not keys.issubset(supported):
        raise KeyError(f"Unsupported keys {keys - supported}")


class _BaseInterfaceMixin(abc.ABC, Generic[T]):

    def insert(self, item: T):
        """Add a new item to the database"""
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

    def retire(self, identity: str):
        """Retire the agent"""
        return self._retire(identity)

    @abc.abstractmethod
    def _retire(self, identity: str):
        raise NotImplementedError()


class ContextBase(_BaseInterfaceMixin[Context]):
    """Base class for Context data"""


class EnsembleStateInconsistentError(RuntimeError):

    def __init__(
        self,
        identity: str,
        expected: Union[EnsembleState, Sequence[EnsembleState]],
        actual: Optional[EnsembleState],
    ):
        if isinstance(expected, EnsembleState):
            expected = (expected,)
        else:
            expected = tuple(expected)
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
    def expected(self) -> Sequence[EnsembleState]:
        return self._expected

    @property
    def actual(self) -> Optional[EnsembleState]:
        return self._actual


class TaskBase(abc.ABC):
    """Base class for Task data"""

    def __init__(self, ensemble_identity: str):
        self._ensemble_identity = ensemble_identity

    @abc.abstractmethod
    def num_failed(self) -> int:
        raise NotImplementedError()

    @abc.abstractmethod
    def num_passed(self) -> int:
        raise NotImplementedError()

    @abc.abstractmethod
    def num_running(self) -> int:
        raise NotImplementedError()

    @abc.abstractmethod
    def num_timedout(self) -> int:
        raise NotImplementedError()

    def num_total(self) -> int:
        return (
            self.num_failed()
            + self.num_passed()
            + self.num_running()
            + self.num_timedout()
        )

    @abc.abstractmethod
    def _new_task(self, task: Task):
        raise NotImplementedError()

    def new_task(self, args: Union[str, NoneType]) -> str:
        """Create task for the ensemble"""
        task = Task.new(
            ensemble_identity=self._ensemble_identity,
            args=args or "",
        )
        self._new_task(task)

        return task.identity

    @abc.abstractmethod
    def _set_task_result(
        self,
        identity: str,
        return_value: Optional[int] = None,
        execution_context_identity: Optional[str] = None,
    ):
        raise NotImplementedError()

    def set_task_result(
        self,
        identity: str,
        return_value: Optional[int],
        execution_context_identity: Optional[str] = None,
    ) -> TaskState:
        """Set the result of the task"""
        return self._set_task_result(identity, return_value, execution_context_identity)

    @abc.abstractmethod
    def _retire(self):
        raise NotImplementedError()

    def retire(self):
        return self._retire()

    def get(self, task_identity: str) -> Optional[Task]:
        return self._get(task_identity)

    @abc.abstractmethod
    def _get(self, task_identity: str) -> Optional[Task]:
        raise NotImplementedError()

    @abc.abstractmethod
    def _list_by_taskstate(self, task_state: TaskState):
        raise NotImplementedError()

    def list_by_taskstate(self, task_state: TaskState):
        return self._list_by_taskstate(task_state)


class EnsembleBase(_BaseInterfaceMixin[Ensemble]):
    """Base class for Ensemble data"""

    @abc.abstractmethod
    def _update_state(
        self,
        identity: str,
        new_state: EnsembleState,
        expected_old_state: Optional[
            Union[EnsembleState, Sequence[EnsembleState]]
        ] = None,
    ):
        raise NotImplementedError()

    def pause(self, identity: str):
        """Pause the ensemble"""
        self._update_state(identity, EnsembleState.STOPPED, EnsembleState.RUNNABLE)

    def resume(self, identity: str):
        """Resume the ensemble"""
        self._update_state(identity, EnsembleState.RUNNABLE, EnsembleState.STOPPED)

    def kill(self, identity: str):
        """Kill the ensemble"""
        self._update_state(
            identity,
            EnsembleState.KILLED,
            (EnsembleState.RUNNABLE, EnsembleState.STOPPED),
        )

    @abc.abstractmethod
    def _add_ensemble_task(
        self, ensemble_identity: str, args: Union[str, NoneType]
    ) -> str:
        raise NotImplementedError()

    def add_ensemble_task(
        self, ensemble_identity: str, args: Union[str, NoneType]
    ) -> str:
        return self._add_ensemble_task(ensemble_identity, args)

    @abc.abstractmethod
    def _set_ensemble_task_result(
        self,
        ensemble_identity: str,
        task_identity: str,
        return_value: Optional[int],
        execution_context_identity: Optional[str] = None,
    ):
        raise NotImplementedError()

    def set_ensemble_task_result(
        self,
        ensemble_identity: str,
        task_identity: str,
        return_value: Optional[int],
        execution_context_identity: Optional[str] = None,
    ):
        self._set_ensemble_task_result(
            ensemble_identity, task_identity, return_value, execution_context_identity
        )

    def get_task(self, ensemble_identity: str, task_identity: str) -> Optional[Task]:
        """Get the task by its identity"""
        return self._get_task(ensemble_identity, task_identity)

    @abc.abstractmethod
    def _get_task(self, ensemble_identity: str, task_identity: str) -> Optional[Task]:
        raise NotImplementedError()

    def iterate_tasks(
        self, ensemble_identity: str, task_state: Optional[TaskState] = None
    ) -> Generator[Task, None, None]:
        """Iterate over all tasks in the ensemble"""
        return self._iterate_tasks(ensemble_identity, task_state)

    @abc.abstractmethod
    def _iterate_tasks(
        self, ensemble_identity: str, task_state: Optional[TaskState]
    ) -> Generator[Task, None, None]:
        raise NotImplementedError()


class AgentBase(_BaseInterfaceMixin[Agent]):
    """Base class for Agents"""

    def heartbeat(self, agent_item: Agent):
        return self._heartbeat(agent_item)

    @abc.abstractmethod
    def _heartbeat(self, agent_item: Agent):
        raise NotImplementedError()


class EnsembleMissingError(RuntimeError):

    def __init__(self, identity: str):
        super().__init__(f"Missing ensemble {identity}")

        self._identity = identity

    @property
    def identity(self) -> str:
        return self._identity


class EnsembleNotRunnableError(EnsembleStateInconsistentError):

    def __init__(self, identity: str, state: Optional[EnsembleState] = None):
        super().__init__(identity, EnsembleState.RUNNABLE, state)

class RecordBase(abc.ABC):

    def __init__(self):
        self._ensemble: Optional[EnsembleBase] = None
        self._context: Optional[ContextBase] = None
        self._agent: Optional[AgentBase] = None

    @abc.abstractmethod
    def _expire_retired_data(self):
        raise NotImplementedError()

    def expire_retired_data(self):
        self._expire_retired_data()

    @property
    @abc.abstractmethod
    def ensemble(self) -> EnsembleBase:
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def context(self) -> ContextBase:
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def agent(self) -> AgentBase:
        raise NotImplementedError()
