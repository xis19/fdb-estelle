import threading

from loguru import logger

from . import Agent
from ..record import record


class AgentInfo:
    """Stores the statistics information about the agent"""

    def __init__(self):
        self._lock = threading.RLock()
        self._agent = Agent.new()

        record.agent.insert(self._agent)
        logger.info(f"Registered agent {self._agent.identity}")

    def task_succeed(self):
        with self._lock:
            self._agent.tasks_succeed += 1

    def tasks_failed(self):
        with self._lock:
            self._agent.tasks_failed += 1

    def heartbeat(self):
        with self._lock:
            record.agent.heartbeat(self._agent.identity)

    def retire(self):
        with self._lock:
            record.agent.retire(self._agent.identity)


agent_info: AgentInfo = AgentInfo()
