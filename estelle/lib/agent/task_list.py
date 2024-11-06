import threading

from typing import Dict, Optional, Tuple

from ..context import Context
from ..task import Task


class TaskList:
    """Stores the tasks for the agent to execute"""

    def __init__(self):
        self._lock = threading.RLock()
        self._task_list: Dict[str, Tuple[Task, Context]] = dict()

    def add_task(self, task: Task, context: Context):
        assert isinstance(task, Task)
        assert isinstance(context, Context)

        identity = task.identity
        with self._lock:
            if identity not in self._task_list:
                self._task_list[identity] = (task, context)

    def take_task(self) -> Optional[Tuple[Task, Context]]:
        with self._lock:
            if len(self._task_list) == 0:
                return None
            return self._task_list.popitem()[1]

    def drop_all(self):
        with self._lock:
            self._task_list.clear()

    def __len__(self) -> int:
        with self._lock:
            return len(self._task_list)


task_list: TaskList = TaskList()
