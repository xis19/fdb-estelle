import dataclasses
import uuid

from typing import Union


@dataclasses.dataclass
class Context:
    identity: str
    owner: str
    size: int
    checksum: Union[str, None]
    tag: str = ""

    @staticmethod
    def new(
        owner: str, size: int, checksum: Union[str, None], tag: str = ""
    ) -> "Context":
        return Context(
            identity=uuid.uuid4().hex,
            size=size,
            owner=owner,
            checksum=checksum,
            tag=tag,
        )
