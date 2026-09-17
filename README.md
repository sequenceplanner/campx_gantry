# gantry driver

An OPC UA client that mirrors the gantry PLC into Redis: it subscribes to the
PLC's nodes and publishes them as `opc_*` keys, and it writes the `opc_write_*`
keys back to the PLC. Anything that can talk to Redis (Python, a web UI, another
service) can therefore drive the gantry without speaking OPC UA.

## Quickstart (Docker)

No Rust toolchain needed — Docker builds it.

```bash
git clone <this repo> && cd campx_gantry
cp .env.example .env
$EDITOR .env          # set OPC_URI to your PLC
docker compose up -d --build
docker compose logs -f gantry
```

That starts two containers:

| Service  | What it is                                   |
| -------- | -------------------------------------------- |
| `redis`  | the state store, published on `localhost:6379` |
| `gantry` | this driver, connecting to your PLC over OPC UA |

Stop with `docker compose down`. The first build takes a few minutes; later
builds are cached.

## Variables to change

Everything lives in `.env` (copied from `.env.example`):

| Variable          | Default                       | Change it to                                     |
| ----------------- | ----------------------------- | ------------------------------------------------ |
| `OPC_URI`         | `opc.tcp://192.168.1.33:4840` | your PLC's OPC UA endpoint — **this is the one you must set** |
| `OPC_NAMESPACE`   | `4`                           | the namespace index the gantry nodes live in     |
| `REDIS_HOST_PORT` | `6379`                        | the host port Redis is published on, if 6379 is taken |
| `RUST_LOG`        | `info`                        | `debug` or `trace` when something misbehaves     |

`REDIS_HOST` / `REDIS_PORT` are set by `docker-compose.yml` to `redis:6379`, the
address inside the compose network. Leave them alone.

If the OPC server runs on the Docker host itself rather than on the network, use
`OPC_URI=opc.tcp://host.docker.internal:4840`.

## Redis keys

The driver reads and writes these keys. Node IDs are hardcoded in
`src/main.rs:139`; changing them needs a rebuild (`docker compose up -d --build`).

| Redis key                      | OPC node | Type  | Direction        |
| ------------------------------ | -------- | ----- | ---------------- |
| `opc_start_flag`               | `i=45`   | bool  | PLC → Redis      |
| `opc_reference_position`       | `i=46`   | float | PLC → Redis      |
| `opc_reference_speed`          | `i=47`   | float | PLC → Redis      |
| `opc_done_flag`                | `i=94`   | bool  | PLC → Redis      |
| `opc_current_position`         | `i=306`  | float | PLC → Redis      |
| `opc_write_start_flag`         | `i=45`   | bool  | Redis → PLC      |
| `opc_write_reference_position` | `i=46`   | float | Redis → PLC      |
| `opc_write_reference_speed`    | `i=47`   | float | Redis → PLC      |

Values are JSON-encoded `micro_sp` `SPValue`s, not bare numbers:

```json
{"type": "Float64", "value": {"Float64": 500.0}}
{"type": "Bool",    "value": {"Bool": true}}
{"type": "Float64", "value": "UNKNOWN"}
```

The driver also mirrors `opc_current_position` into a transform named `vagn`, if
one exists in Redis. If it doesn't, that update is skipped silently.

## Python example

Changing a variable means writing an `SPValue` to the matching Redis key.
Full script in [`examples/gantry_client.py`](examples/gantry_client.py).

```bash
pip install redis
python examples/gantry_client.py
```

The script connects to `127.0.0.1:6379` by default, and honours `REDIS_HOST` /
`REDIS_PORT` if you moved Redis elsewhere.

```python
import json
import time

import redis

r = redis.Redis(host="127.0.0.1", port=6379, decode_responses=True)


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
```

Poking at it by hand works too:

```bash
docker compose exec redis redis-cli get opc_current_position
docker compose exec redis redis-cli set opc_write_reference_speed \
  '{"type":"Float64","value":{"Float64":100.0}}'
```

## Running without Docker

Needs a Rust toolchain (edition 2024, so 1.85+) and a Redis reachable via
`REDIS_HOST` / `REDIS_PORT` (default `127.0.0.1:6379`).

```
$ cargo run -- --help
Usage: gantry [OPTIONS]

Options:
      --opc-uri <OPC_URI>  Opc server URI [env: OPC_URI=] [default: opc.tcp://192.168.1.33:4840]
      --opc-ns <OPC_NS>    Opc server namespace [env: OPC_NAMESPACE=] [default: 4]
  -h, --help               Print help
  -V, --version            Print version
```

```
$ REDIS_HOST=192.168.1.15 cargo run
sp state
  opc_write_start_flag - false
  opc_write_reference_position - 0.0
  opc_write_reference_speed - 0.0

to write: 45 - false
WARNING: cannot write to 45 we havent read the item yet
to write: 46 - 0.0
WARNING: cannot write to 46 we havent read the item yet
to write: 47 - 0.0
WARNING: cannot write to 47 we havent read the item yet
opc state
  opc_done_flag - false
  opc_current_position - 500.0
  opc_start_flag - false
  opc_reference_speed - 100.0
  opc_reference_position - 500.0
```

The `cannot write ... we havent read the item yet` warnings at startup are
expected: the driver needs one subscription update per node to learn its OPC
data type before it can write to it.
