import time

from loguru import logger

from ..config import config
from .agent_info import agent_info


def heartbeat():
    while True:
        agent_info.heartbeat()
        logger.info("Heartbeat triggered")
        time.sleep(config.agent.heartbeat_interval)
