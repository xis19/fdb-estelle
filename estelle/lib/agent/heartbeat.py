import time

from loguru import logger

from .agent_info import agent_info
from ..config import config


def heartbeat():
    while True:
        agent_info.heartbeat()
        logger.info("Heartbeat triggered")
        time.sleep(config.agent.heartbeat_interval)
