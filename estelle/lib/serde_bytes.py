import dataclasses
import datetime
import enum
import struct
import typing
from typing import Any, ClassVar, Dict, Optional, Protocol, TypeVar, Union

from loguru import logger


class DataclassProtocol(Protocol):
    __dataclass_fields__: ClassVar[Dict[str, Any]]


T = TypeVar("T")


def serialize_by_type(value: Any) -> bytes:
    if isinstance(value, bytes):
        return value
    elif isinstance(value, str):
        return value.encode()
    # NOTE: True/False are also instance of int
    elif isinstance(value, bool):
        return struct.pack("?", value)
    elif isinstance(value, int):
        return struct.pack("<q", value)
    elif isinstance(value, float):
        return struct.pack("<d", value)
    elif value is None:
        return b""
    elif isinstance(value, datetime.datetime):
        return serialize_by_type(value.timestamp())
    elif enum.IntEnum in type(value).mro():
        return serialize_by_type(value.value)

    raise TypeError(f"Unsupported type {type(value)} for value {value}")


def deserialize_by_type(raw: bytes, t: type[T]) -> Optional[T]:
    if len(raw) == 0:
        return None

    if t is bytes:
        assert type(raw) is t
        return raw
    elif t is int:
        return struct.unpack("<q", raw)[0]
    elif t is str:
        result = raw.decode()
        assert type(result) is t
        return result
    elif t is float:
        return struct.unpack("<d", raw)[0]
    elif t is bool:
        return struct.unpack("?", raw)[0]
    elif t is datetime.datetime:
        float_value = deserialize_by_type(raw, float)
        assert isinstance(float_value, float)
        result = datetime.datetime.fromtimestamp(float_value).astimezone(
            datetime.timezone.utc
        )
        assert type(result) is t
        return result
    elif hasattr(t, "mro") and enum.IntEnum in t.mro():
        assert issubclass(t, enum.IntEnum)
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


def deserialize(dc: type[DC], raw: Dict[bytes, bytes]) -> DC:
    deserialized: Dict[str, Any] = dict()
    dc_keys = {
        field.name: _uncrustify_type(field.type) for field in dataclasses.fields(dc)
    }

    for k, v in raw.items():
        str_k = k.decode()
        if str_k not in dc_keys:
            logger.warning(f"Key {str_k} is not in data class {dc}")
        else:
            deserialized[str_k] = deserialize_by_type(v, dc_keys[str_k])  # type: ignore
            dc_keys.pop(str_k, None)

    if len(dc_keys) > 0:
        for k in dc_keys:
            logger.error(f"Key {k} not defined in raw dict")
        raise KeyError("Missing keys")

    return dc(**deserialized)
