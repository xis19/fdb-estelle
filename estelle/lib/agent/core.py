import random
import threading

from loguru import logger

from .heartbeat import heartbeat
from .loader import task_loader
from .runner import task_runner


def agent_core():
    random.seed()

    heartbeat_thread = threading.Thread(
        target=heartbeat, args=tuple(), daemon=True, name="heartbeat"
    )
    heartbeat_thread.start()
    logger.info("Heartbeat thread started")

    task_load_thread = threading.Thread(
        target=task_loader, args=tuple(), daemon=True, name="loader"
    )
    task_load_thread.start()
    logger.info("Task load thread started")

    task_runner_thread = threading.Thread(
        target=task_runner, args=tuple(), daemon=True, name="runner"
    )
    task_runner_thread.start()
    logger.info("Task runner thread started")

    threads = (heartbeat_thread, task_load_thread, task_runner_thread)

    alive = True
    while alive:
        for thread in threads:
            if not thread.is_alive():
                logger.info(f"Thread {thread.name} terminated")
                alive = False

    logger.info("Agent quitting...")
