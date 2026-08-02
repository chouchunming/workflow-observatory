from __future__ import annotations

import hashlib
import json

_MIN_SAFE_INTEGER = -(2**53) + 1
_MAX_SAFE_INTEGER = (2**53) - 1


class CanonicalizationError(ValueError):
    pass


def _validate_string(value: str) -> None:
    if any(0xD800 <= ord(character) <= 0xDFFF for character in value):
        raise CanonicalizationError("lone surrogate is not valid I-JSON")


def _string_bytes(value: str) -> bytes:
    _validate_string(value)
    return json.dumps(
        value, ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")


def _key_order(value: str) -> bytes:
    _validate_string(value)
    return value.encode("utf-16-be")


def _encode(value: object) -> bytes:
    if value is None:
        return b"null"
    if value is True:
        return b"true"
    if value is False:
        return b"false"
    if isinstance(value, int):
        if not _MIN_SAFE_INTEGER <= value <= _MAX_SAFE_INTEGER:
            raise CanonicalizationError("integer is outside the I-JSON safe range")
        return str(value).encode("ascii")
    if isinstance(value, float):
        raise CanonicalizationError("JSON floating-point numbers are prohibited")
    if isinstance(value, str):
        return _string_bytes(value)
    if isinstance(value, dict):
        if not all(isinstance(key, str) for key in value):
            raise CanonicalizationError("JSON object keys must be strings")
        keys = sorted(value, key=_key_order)
        return b"{" + b",".join(
            _string_bytes(key) + b":" + _encode(value[key]) for key in keys
        ) + b"}"
    if isinstance(value, list):
        return b"[" + b",".join(_encode(item) for item in value) + b"]"
    raise CanonicalizationError(f"unsupported JSON value: {type(value).__name__}")


def canonicalize(value: object) -> bytes:
    return _encode(value)


def strict_json_loads(data: str | bytes) -> object:
    if isinstance(data, bytes):
        try:
            text = data.decode("utf-8", errors="strict")
        except UnicodeDecodeError as error:
            raise CanonicalizationError("JSON input is not valid UTF-8") from error
    elif isinstance(data, str):
        text = data
    else:
        raise CanonicalizationError("JSON input must be text or UTF-8 bytes")

    def unique_object(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise CanonicalizationError(f"duplicate JSON key: {key}")
            value[key] = item
        return value

    def reject_constant(token):
        raise CanonicalizationError(f"non-finite JSON number: {token}")

    try:
        value = json.loads(
            text,
            object_pairs_hook=unique_object,
            parse_constant=reject_constant,
        )
    except (CanonicalizationError, json.JSONDecodeError) as error:
        if isinstance(error, CanonicalizationError):
            raise
        raise CanonicalizationError("invalid JSON input") from error
    canonicalize(value)
    return value


def hash_canonical(domain: bytes, value: object) -> str:
    if not isinstance(domain, bytes) or not domain.endswith(b"\0"):
        raise CanonicalizationError("hash domain must be NUL-terminated bytes")
    return hashlib.sha256(domain + canonicalize(value)).hexdigest()
