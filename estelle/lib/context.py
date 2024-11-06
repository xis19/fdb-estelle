import dataclasses
import uuid


@dataclasses.dataclass
class Context:
    identity: str
    owner: str
    size: int
    checksum: str
    tag: str = ""

    @staticmethod
    def new(owner: str, size: int, checksum: str, tag: str = "") -> "Context":
        return Context(
            identity=uuid.uuid4().hex,
            size=size,
            owner=owner,
            checksum=checksum,
            tag=tag,
        )
