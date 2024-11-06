import random
import time

from typing import Optional

from loguru import logger

from .task_list import task_list
from ..config import config
from ..context import Context
from ..ensemble import Ensemble, EnsembleState
from ..model import list_ensemble, get_context_by_identity
from ..task import Task


def _get_ensemble() -> Optional[Ensemble]:
    """Gets an ensemble to run"""
    candidates = tuple(list_ensemble(EnsembleState.RUNNABLE))
    if len(candidates) > 0:
        return random.choices(
            candidates, weights=[abs(c.priority) + 1 for c in candidates], k=1
        )[0]

    return None


def _prepare_tasks(ensemble_identity: str, context: Context) -> int:
    num_tasks = config.agent.tasks_per_ensemble
    for _ in range(num_tasks):
        task_list.add_task(
            Task.new(
                ensemble_identity=ensemble_identity,
                args=None,
            ),
            context,
        )
    return num_tasks


def task_loader():
    while True:
        logger.info("Start loading task")

        ensemble = _get_ensemble()
        if ensemble is None:
            logger.info("No runnable ensemble")
            time.sleep(config.agent.no_ensemble_runnable_waiting_period)
            continue

        context = get_context_by_identity(identity=ensemble.context_identity)
        assert isinstance(context, Context)

        if context is None:
            logger.error(
                f"Ensemble {ensemble.identity} has {ensemble.context_identity} which does not exist"
            )
            continue

        num_tasks_created = _prepare_tasks(ensemble.identity, context)
        logger.info(f"Ensemble {ensemble.identity}: {num_tasks_created} tasks created")
        while (task_length := len(task_list)) > 0:
            logger.debug(f"Task list length: {task_length}")
            time.sleep(config.agent.task_list_check_interval)
