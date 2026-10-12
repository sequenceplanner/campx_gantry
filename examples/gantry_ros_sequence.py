"""Move the gantry through three positions in a row via the `ros` action server.

Each move is sent only after the previous one has finished; the sequence stops
at the first move that does not succeed.

    source /opt/ros/lyrical/setup.bash
    colcon build --base-paths interfaces && source install/setup.bash
    python3 examples/gantry_ros_sequence.py
"""

import rclpy
from rclpy.action import ActionClient
from rclpy.node import Node

from gantry_interfaces.action import Move

# (position, speed) for each move, in the PLC's units.
MOVES = [
    (800.0, 200.0),
    (1500.0, 100.0),
    (200.0, 250.0),
]


def move(node, client, position, speed):
    goal = Move.Goal(position=position, speed=speed)
    send_future = client.send_goal_async(
        goal,
        feedback_callback=lambda fb: print(f"  [{fb.feedback.status}] at:", fb.feedback.current_position),
    )
    rclpy.spin_until_future_complete(node, send_future)
    handle = send_future.result()
    if not handle.accepted:
        print("  rejected: another move is running")
        return False

    result_future = handle.get_result_async()
    rclpy.spin_until_future_complete(node, result_future)
    result = result_future.result().result
    if result.success:
        print("  done at", result.position)
    else:
        print("  failed at", result.position, "-", result.message)
    return result.success


def main():
    rclpy.init()
    node = Node("gantry_sequence")
    client = ActionClient(node, Move, "/gantry/move")
    client.wait_for_server()

    for i, (position, speed) in enumerate(MOVES, start=1):
        print(f"move {i}/{len(MOVES)}: to {position} at {speed}")
        if not move(node, client, position, speed):
            break

    node.destroy_node()
    rclpy.shutdown()


if __name__ == "__main__":
    main()
