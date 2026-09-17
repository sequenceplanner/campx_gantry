"""Minimal example: drive the gantry by changing variables in Redis.

    pip install redis
    python examples/gantry_client.py

The driver keeps Redis in sync with the PLC:
  * it writes what it reads from OPC UA into the `opc_*` keys
  * it reads the `opc_write_*` keys and pushes them to OPC UA
"""

import json
import os
import time

import redis

# Defaults match docker-compose: Redis published on localhost:6379.
r = redis.Redis(
    host=os.environ.get("REDIS_HOST", "127.0.0.1"),
    port=int(os.environ.get("REDIS_PORT", 6379)),
    decode_responses=True,
)


# --- micro_sp value encoding ------------------------------------------
# Values are JSON-encoded SPValues, e.g.
#   {"type": "Float64", "value": {"Float64": 500.0}}
#   {"type": "Bool",    "value": {"Bool": true}}
# An unset value is {"type": "Float64", "value": "UNKNOWN"}.

def sp_float(x):
    return json.dumps({"type": "Float64", "value": {"Float64": float(x)}})


def sp_bool(b):
    return json.dumps({"type": "Bool", "value": {"Bool": bool(b)}})


def read(key):
    """Return the plain Python value behind `key`, or None if unset/unknown."""
    raw = r.get(key)
    if raw is None:
        return None
    value = json.loads(raw)["value"]
    if value == "UNKNOWN":
        return None
    return next(iter(value.values()))


def main():
    print("current position:", read("opc_current_position"))

    # Command a move: set the setpoints first, then raise the start flag.
    r.mset({
        "opc_write_reference_position": sp_float(500.0),
        "opc_write_reference_speed": sp_float(100.0),
    })
    r.set("opc_write_start_flag", sp_bool(True))

    # Wait for the PLC to report it is done.
    while not read("opc_done_flag"):
        print("moving, at:", read("opc_current_position"))
        time.sleep(0.2)

    # Drop the start flag so the next move can be triggered.
    r.set("opc_write_start_flag", sp_bool(False))
    print("done, at:", read("opc_current_position"))


if __name__ == "__main__":
    main()
