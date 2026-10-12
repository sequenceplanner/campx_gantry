# gantry driver

An OPC UA client that mirrors the gantry PLC into Redis: it subscribes to the
PLC's nodes and publishes them as `opc_*` keys, and it writes the `opc_write_*`
keys back to the PLC. Anything that can talk to Redis (Python, a web UI, another
service) can therefore drive the gantry without speaking OPC UA.

A second executable, `ros`, puts a ROS 2 action server in front of those same
Redis keys, so a ROS 2 client can command moves too (see [ROS 2](#ros-2)).

## Quickstart (Docker)

No Rust toolchain needed — Docker builds it.

```bash
git clone <this repo> && cd campx_gantry
cp .env.example .env
$EDITOR .env          # set OPC_URI to your PLC
docker compose up -d --build
docker compose logs -f gantry
```

That starts three containers:

| Service  | What it is                                   |
| -------- | -------------------------------------------- |
| `redis`  | the state store, published on `localhost:6379` |
| `gantry` | this driver, connecting to your PLC over OPC UA |
| `ros`    | the ROS 2 action server `/gantry/move` and state topic `/gantry/state`, on the host network |

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
| `ROS_DISTRO`      | `lyrical`                     | the ROS 2 distro the image is built on           |
| `ROS_DOMAIN_ID`   | `0`                           | the domain your ROS 2 clients use                |
| `GANTRY_ACTION_NAME`  | `/gantry/move`            | another action name                              |
| `GANTRY_GOAL_TIMEOUT` | `120`                     | seconds a move may take before the goal aborts   |
| `GANTRY_STATE_TOPIC`  | `/gantry/state`           | another topic for the read values                |

`REDIS_HOST` / `REDIS_PORT` are set by `docker-compose.yml` to `redis:6379`, the
address inside the compose network. Leave them alone.

If the OPC server runs on the Docker host itself rather than on the network, use
`OPC_URI=opc.tcp://host.docker.internal:4840`.

## Redis keys

The driver reads and writes these keys. Node IDs are hardcoded in
`src/main.rs:144`; changing them needs a rebuild (`docker compose up -d --build`).

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

## Python client examples (virtual environment)

One venv runs all the Python examples. It needs `--system-site-packages`
because rclpy and the ROS message packages come from the ROS install and can't
be installed with pip. Without that flag the ROS examples fail with
`No module named 'em'`. For the same reason, create the venv with the system
`python3` that ROS was built for, not a pyenv or conda one.

```bash
sudo apt install python3-venv        # once, if `python3 -m venv` complains about ensurepip
python3 -m venv --system-site-packages .venv
source .venv/bin/activate
pip install -r examples/requirements.txt
```

For the ROS examples, load ROS and `gantry_interfaces` too. The order
relative to `activate` doesn't matter:

```bash
source /opt/ros/lyrical/setup.bash
colcon build --base-paths interfaces && source install/setup.bash
python examples/gantry_ros_sequence.py
```

## Looking at and changing values with RedisInsight

[RedisInsight](https://redis.io/insight/) is a GUI for Redis. Either install
the desktop app, or run it in Docker on the host network and open
<http://localhost:5540>:

```bash
docker run -d --name redisinsight --network host redis/redisinsight:latest
```

**Connect.** Add a database with host (find IP of the redis docker with 'ifconfig') and port `6379` (or your
`REDIS_HOST_PORT`), no username or password.

**Look.** Open *Browser* and type `opc_*` in the key filter. Click a key to see
its value. The values are strings that hold JSON, so pick *JSON* in the
value's format dropdown to get them pretty-printed. Use the refresh button
(or its auto-refresh option) to follow `opc_current_position` during a move.

**Change.** Only edit the `opc_write_*` keys; those are what the driver sends
to the PLC. The `opc_*` keys without `write` are the PLC's read values, and the
driver overwrites them on the next update. Click the pencil icon on the value,
replace it with a full `SPValue`, and save:

```json
{"type": "Float64", "value": {"Float64": 500.0}}
{"type": "Bool", "value": {"Bool": true}}
```

Keep the `type` the same as the key's (`Float64` for positions and speeds,
`Bool` for the start flag). A value that doesn't parse is ignored by the
driver. Write positions and speeds with a decimal point (`500.0`).

Or type the commands into RedisInsight's *CLI* or *Workbench*:

```
GET opc_current_position
SET opc_write_reference_speed '{"type":"Float64","value":{"Float64":100.0}}'
```

A move by hand follows the same order as the ROS server: set
`opc_write_start_flag` to `false`, wait until `opc_start_flag` reads `false`,
set `opc_write_reference_position` and `opc_write_reference_speed`, wait for
their read values to match, then set `opc_write_start_flag` to `true` and watch
`opc_done_flag`.

Two things to keep in mind:

- The driver only sends a write key to the PLC when its value **changes**.
  Saving the value it already has does nothing, even if the PLC shows
  something else. Change it to something else and back.
- Don't edit the `opc_write_*` keys while a ROS goal is running. The action
  server writes the same keys, and the goal will abort or misbehave.



## ROS 2

The `ros` executable is an action server of type
`gantry_interfaces/action/Move`, defined in
[`interfaces/gantry_interfaces`](interfaces/gantry_interfaces/action/Move.action):

```
float64 position          # goal
float64 speed
---
bool success              # result
string message            # why it failed; empty on success
float64 position          # where the gantry ended up
---
float64 current_position  # feedback, ~10 Hz
```

Each goal goes through the same Redis keys as the Python example above, one
step at a time. Before each next step it waits until the read values (the keys
without `write`) show the PLC has the previous one:

1. `opc_write_start_flag` low, until `opc_start_flag` reads low.
2. `opc_write_reference_position` / `_speed`, until `opc_reference_*` match.
3. `opc_write_start_flag` high, until `opc_start_flag` reads high.
4. `opc_current_position` is published as feedback until `opc_done_flag` rises.
   A done flag still up from the previous move has to drop first before it counts.
5. `opc_write_start_flag` low again, also when the goal is canceled or aborted.

It also publishes the read values at ~10 Hz on `/gantry/state`, type
`gantry_interfaces/msg/State`, whether or not a goal is running. Nothing is
published until all five keys are in Redis, i.e. until the driver has connected.

```
bool start_flag
float64 reference_position
float64 reference_speed
bool done_flag
float64 current_position
```

- It needs the `gantry` driver running; without it the goal aborts after 5 s
  with "the PLC did not acknowledge the setpoints".
- One move at a time: a goal sent while another runs is rejected.
- Canceling a goal drops the start flag. What the PLC does then is up to the PLC.
- A move that takes longer than `GANTRY_GOAL_TIMEOUT` seconds is aborted.

Clients need `gantry_interfaces` built and sourced to know the action type:

```bash
source /opt/ros/lyrical/setup.bash
colcon build --base-paths interfaces
source install/setup.bash

ros2 action send_goal /gantry/move gantry_interfaces/action/Move \
  "{position: 500.0, speed: 100.0}" --feedback
ros2 topic echo /gantry/state

python3 examples/gantry_ros_client.py 500 100   # rclpy version
python3 examples/gantry_ros_sequence.py         # three moves in a row
```

Or use the image, which has both, without installing anything:

```bash
docker compose run --rm --no-deps ros \
  ros2 action send_goal /gantry/move gantry_interfaces/action/Move \
  "{position: 500.0, speed: 100.0}" --feedback
```

The `ros` container uses host networking so clients on this machine and the LAN
can discover it, and it forces Fast DDS onto UDP, since shared memory between a
container and the host discovers peers but moves no data.