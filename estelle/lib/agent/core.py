import random
import threading

from loguru import logger

from .heartbeat import heartbeat
from .worker import worker


def agent_core():
    random.seed()

    heartbeat_thread = threading.Thread(
        target=heartbeat, args=tuple(), daemon=True, name="heartbeat"
    )
    heartbeat_thread.start()
    logger.info("Heartbeat thread started")

    work_thread = threading.Thread(
        target=worker, args=tuple(), daemon=True, name="loader"
    )
    work_thread.start()
    logger.info("Task load thread started")

    work_thread.join()
    logger.info("Agent quitting...")
