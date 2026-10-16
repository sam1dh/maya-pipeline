"""Parse and validate one reply line from the sensor mock (spec Section 6.2)."""
import json
import zlib

WIDTH = {1: 3, 2: 2}   # channels per sample for each sensor


class ProtocolError(Exception):
    """A reply that must not be used (bad JSON, wrong CRC, wrong shape)."""


def raw_data_text(line: bytes) -> bytes:
    """Return the exact bytes of the "data" array, from '[' to its matching ']'.

    The CRC is computed over the data text exactly as sent, so we must check
    these original bytes, never a re-serialised version of the parsed JSON.
    """
    key = line.find(b'"data"')
    if key < 0:
        raise ProtocolError("no data field")
    start = line.find(b"[", key)
    if start < 0 or line[key + 6:start].strip() != b":":
        raise ProtocolError("data field is not an array")
    depth = 0
    for pos in range(start, len(line)):
        ch = line[pos]
        if ch == 0x5B:        # '['
            depth += 1
        elif ch == 0x5D:      # ']'
            depth -= 1
            if depth == 0:
                return line[start:pos + 1]
    raise ProtocolError("data array is not terminated")


def parse_reply(line: bytes) -> dict:
    """Return the reply as a dict, or raise ProtocolError if it must not be used."""
    try:
        msg = json.loads(line)
    except ValueError as e:
        raise ProtocolError(f"not valid JSON ({e})") from None
    if not isinstance(msg, dict):
        raise ProtocolError("reply is not a JSON object")
    if "error" in msg:
        raise ProtocolError(f"mock returned an error: {msg['error']}")
    for field in ("session_id", "sensor", "first_index", "count", "lost", "crc32", "data"):
        if field not in msg:
            raise ProtocolError(f"missing field '{field}'")

    raw = raw_data_text(line)
    try:
        expected = int(str(msg["crc32"]), 16)
    except ValueError:
        raise ProtocolError("crc32 is not hexadecimal") from None
    if zlib.crc32(raw) & 0xFFFFFFFF != expected:
        raise ProtocolError("crc32 mismatch (corrupt reply)")

    sensor, data = msg["sensor"], msg["data"]
    if sensor not in WIDTH:
        raise ProtocolError(f"unknown sensor {sensor!r}")
    if not isinstance(msg["first_index"], int) or msg["first_index"] < 0:
        raise ProtocolError("bad first_index")
    if not isinstance(msg["lost"], list):
        raise ProtocolError("lost is not a list")
    if len(data) != msg["count"]:
        raise ProtocolError("count does not match number of samples")
    for s in data:
        if not isinstance(s, list) or len(s) != WIDTH[sensor] \
                or not all(type(v) is int for v in s):
            raise ProtocolError("sample has wrong channels or non-integer values")
    return msg
