import threading
from typing import Optional

from loguru import logger

from ..record import record
from . import Agent, TaskExecuteStage


class AgentInfo:
    """Stores the statistics information about the agent"""

    def __init__(self):
        self._lock = threading.RLock()
        self._agent = Agent.new()

        record.agent.insert(self._agent)
        logger.info(f"Registered agent {self._agent.identity}")

    def get_current_ensemble_identity(self) -> Optional[str]:
        with self._lock:
            return self._agent.current_ensemble

    def set_current_ensemble(self, ensemble_identity: str):
        with self._lock:
            self._agent.current_ensemble = ensemble_identity
            record.agent.heartbeat(self._agent)

    def set_current_task(self, task_identity: Optional[str] = None):
        with self._lock:
            self._agent.current_task = task_identity
            record.agent.heartbeat(self._agent)

    def set_current_task_stage(self, stage: TaskExecuteStage):
        with self._lock:
            self._agent.current_task_stage = stage
            record.agent.heartbeat(self._agent)

    def task_succeed(self):
        with self._lock:
            self._agent.tasks_succeed += 1

    def tasks_failed(self):
        with self._lock:
            self._agent.tasks_failed += 1

    def heartbeat(self):
        with self._lock:
            record.agent.heartbeat(self._agent)

    def retire(self):
        with self._lock:
            record.agent.retire(self._agent.identity)


agent_info: AgentInfo = AgentInfo()
