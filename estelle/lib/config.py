import dataclasses
import pathlib

from typing import Literal, Optional

import platformdirs
import platformdirs.unix
import serde
import serde.toml


_CONFIG_FILE_NAME: str = "config.toml"
_CONFIG_DIR: str = platformdirs.user_config_dir("fdb-estelle", ensure_exists=True)
_CONFIG_PATH: pathlib.Path = pathlib.Path(_CONFIG_DIR).joinpath(_CONFIG_FILE_NAME)


@serde.serde
@dataclasses.dataclass
class EnsembleConfig:
    default_timeout: int = 5400
    """ Seconds a ensemble should take """

    default_tasks: int = 100000
    """ Number of runs of the test """

    default_tolerable_failures: int = 10
    """ Number of failures an ensemble could accept """


@serde.serde
@dataclasses.dataclass
class RecordConfig:
    backend_type: Literal["sql"] = "sql"
    """Database backend"""

    sql_connect_string: Optional[str] = "sqlite:///estelle.sqlite3"
    cluster_file_path: Optional[str] = None


@serde.serde
@dataclasses.dataclass
class StorageConfig:
    backend_type: Literal["local_filesystem"] = "local_filesystem"
    """Object storage backend"""

    read_buffer_size: Optional[int] = 1024 * 1024
    write_buffer_size: Optional[int] = 1024 * 1024
    s3_bucket: Optional[str] = None
    local_cache_directory: Optional[pathlib.Path] = pathlib.Path(
        platformdirs.user_cache_path("estelle", ensure_exists=True)
    )
    local_storage_directory: Optional[pathlib.Path] = pathlib.Path(
        platformdirs.user_cache_dir("estelle", ensure_exists=True)
    )


@serde.serde
@dataclasses.dataclass
class AgentConfig:
    tasks_per_ensemble: int = 5
    """ Number of tasks to retrieve when run an ensemble """

    task_generator: Literal["joshua"] = "joshua"
    """ Generator of tasks, currently only joshua is supported """

    no_ensemble_runnable_waiting_period: float = 1.0
    """ When Agent found there is no runnable agent, delay this time after next check """

    task_list_check_interval: float = 1.0
    """ The interval the agent checks its task list """

    max_idle_time: float = 3.0
    """ If the agent does not have task for max_idle_time, it should terminate itself """

    heartbeat_interval: float = 1.0
    """ The interval the agent reports its heartbeat """


@serde.serde
@dataclasses.dataclass
class Config:
    record: RecordConfig = dataclasses.field(default_factory=RecordConfig)
    storage: StorageConfig = dataclasses.field(default_factory=StorageConfig)
    ensemble: EnsembleConfig = dataclasses.field(default_factory=EnsembleConfig)
    agent: AgentConfig = dataclasses.field(default_factory=AgentConfig)


config: Config


def load_config(override_path: pathlib.Path = _CONFIG_PATH):
    global config

    if not override_path.exists():
        with open(override_path, "w") as stream:
            stream.write(serde.toml.to_toml(Config()))

    with open(override_path, "r") as stream:
        config = serde.toml.from_toml(Config, stream.read())


# Always load a basic config
load_config()
