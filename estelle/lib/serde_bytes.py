import dataclasses
import datetime
import enum
import struct
import typing
from typing import Union, Optional, Protocol, ClassVar, Dict, Any, TypeVar

from loguru import logger


class DataclassProtocol(Protocol):
    __dataclass_fields__: ClassVar[Dict[str, Any]]


T = TypeVar("T")


def serialize_by_type(value: Any) -> bytes:
    if isinstance(value, bytes):
        return value
    elif isinstance(value, str):
        return value.encode()
    elif isinstance(value, int):
        return struct.pack("<q", value)
    elif isinstance(value, float):
        return struct.pack("<d", value)
    elif isinstance(value, bool):
        return struct.pack("?", value)
    elif value is None:
        return b""
    elif isinstance(value, datetime.datetime):
        return serialize_by_type(value.timestamp())
    elif enum.IntEnum in type(value).mro():
        return serialize_by_type(value.value)

    raise TypeError(f"Unsupported type {type(value)} for value {value}")


def deserialize_by_type[T](raw: bytes, t: Union[type[T], str]) -> Optional[T]:
    if len(raw) == 0:
        return None

    if t is bytes:
        return raw
    elif t is int:
        return struct.unpack("<q", raw)[0]
    elif t is str:
        return raw.decode()
    elif t is float:
        return struct.unpack("<d", raw)[0]
    elif t is bool:
        return struct.unpack("?", raw)[0]
    elif t is datetime.datetime:
        return datetime.datetime.fromtimestamp(deserialize_by_type(raw, float))
    elif hasattr(t, "mro") and enum.IntEnum in t.mro():
        return t(deserialize_by_type(raw, int))

    raise TypeError(f"Unsupported type: {t}")


def _uncrustify_type(t):
    if typing.get_origin(t) is Union and type(None) in (args := typing.get_args(t)):
        assert len(args) == 2
        for item in args:
            if item is not type(None):
                return item
    return t


def serialize(
    dc: DataclassProtocol,
) -> Dict[bytes, bytes]:
    result: Dict[bytes, bytes] = dict()

    for field in dataclasses.fields(dc):
        b_name = field.name.encode()
        b_value = serialize_by_type(getattr(dc, field.name))

        result[b_name] = b_value

    return result


DC = TypeVar("DC", bound=DataclassProtocol)


def deserialize[DC](dc: type[DC], raw: Dict[bytes, bytes]) -> DC:
    deserialized: Dict[str, Any] = dict()
    dc_keys = {
        field.name: _uncrustify_type(field.type) for field in dataclasses.fields(dc)
    }

    for k, v in raw.items():
        str_k = k.decode()
        if str_k not in dc_keys:
            logger.warning(f"Key {str_k} is not in data class {dc}")
        deserialized[str_k] = deserialize_by_type(v, dc_keys[str_k])
        del dc_keys[str_k]

    if len(dc_keys) > 0:
        for k in dc_keys:
            logger.error(f"Key {k} not defined in raw dict")
        raise KeyError("Missing keys")

    return dc(**deserialized)
