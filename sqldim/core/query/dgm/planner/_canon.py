"""Canonical encoding and hash ``H`` of DGM §8.14.

``H`` is SHA-256 over a length-prefixed, type-tagged encoding, so that no two
distinct inputs share a byte string.  Every value is encoded as::

    tag (1 byte) · length of payload (8 bytes, big-endian) · payload

and a sequence's payload is the concatenation of its encoded items, so the
encoding is prefix-free and injective over the supported types.  Only the
types below are accepted; anything else — a dict, a set, an enum, an
arbitrary object — is a :class:`CanonicalEncodingError`, never a ``repr``.
Callers order unordered collections themselves (§8.14: *sorted by name*).

========  ===================================================
Tag       Value
========  ===================================================
``N``     ``None``
``T/F``   ``True`` / ``False``
``I``     ``int`` — signed decimal ASCII
``D``     finite ``float`` — ``float.hex()``
``S``     ``str`` — UTF-8
``B``     ``bytes``
``L``     ``tuple`` or ``list`` — encoded items in order
========  ===================================================
"""

from __future__ import annotations

import hashlib
import math

__all__ = [
    "CANON_VERSION",
    "CanonicalEncodingError",
    "canonical_encode",
    "canonical_hash",
]

#: Version of the serialisation and normalisation rules (§8.14
#: ``canon_version``).  Any change to the encoding below, to the IR payloads
#: that feed it or to the normalisation applied before hashing must change
#: this value, which retires every earlier key.
CANON_VERSION: str = "dgm-canon/1"

_LEN_BYTES = 8


class CanonicalEncodingError(TypeError):
    """A value has no canonical encoding."""


def _frame(tag: bytes, payload: bytes) -> bytes:
    return tag + len(payload).to_bytes(_LEN_BYTES, "big") + payload


def _encode_scalar(value: object) -> bytes | None:
    if value is None:
        return _frame(b"N", b"")
    if isinstance(value, bool):  # before int: bool is an int subclass
        return _frame(b"T" if value else b"F", b"")
    if isinstance(value, int):
        return _frame(b"I", str(value).encode("ascii"))
    if isinstance(value, float):
        if not math.isfinite(value):
            raise CanonicalEncodingError(f"non-finite float {value!r}")
        return _frame(b"D", value.hex().encode("ascii"))
    if isinstance(value, str):
        return _frame(b"S", value.encode("utf-8"))
    if isinstance(value, bytes):
        return _frame(b"B", value)
    return None


def canonical_encode(value: object) -> bytes:
    """Return the canonical byte string of *value*."""
    scalar = _encode_scalar(value)
    if scalar is not None:
        return scalar
    if isinstance(value, (tuple, list)):
        return _frame(b"L", b"".join(canonical_encode(v) for v in value))
    raise CanonicalEncodingError(
        f"{type(value).__name__} has no canonical encoding; convert it to "
        "None, bool, int, float, str, bytes or a sequence of those"
    )


def canonical_hash(*parts: object) -> str:
    """``H(parts…)`` — SHA-256 of the canonical encoding, full 256-bit hex."""
    return hashlib.sha256(canonical_encode(parts)).hexdigest()
