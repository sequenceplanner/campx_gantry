"""Move the gantry through the `ros` action server.

Needs ROS 2 and the gantry_interfaces package sourced:

    source /opt/ros/lyrical/setup.bash
    colcon build --base-paths interfaces && source install/setup.bash
    python3 examples/gantry_ros_client.py 500 100

Pass --cancel-after SECONDS to cancel the move part way.
"""

import argparse

import rclpy
from rclpy.action import ActionClient
from rclpy.node import Node

from gantry_interfaces.action import Move


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("position", type=float)
    parser.add_argument("speed", type=float)
    parser.add_argument("--action", default="/gantry/move")
    parser.add_argument("--cancel-after", type=float, default=None)
    args = parser.parse_args()

    rclpy.init()
    node = Node("gantry_client")
    client = ActionClient(node, Move, args.action)
    client.wait_for_server()

    goal = Move.Goal(position=args.position, speed=args.speed)
    send_future = client.send_goal_async(
        goal,
        feedback_callback=lambda fb: print("at:", fb.feedback.current_position),
    )
    rclpy.spin_until_future_complete(node, send_future)
    handle = send_future.result()
    if not handle.accepted:
        print("rejected: another move is running")
        return

    result_future = handle.get_result_async()
    if args.cancel_after is not None:
        rclpy.spin_until_future_complete(node, result_future, timeout_sec=args.cancel_after)
        if not result_future.done():
            rclpy.spin_until_future_complete(node, handle.cancel_goal_async())
    rclpy.spin_until_future_complete(node, result_future)

    result = result_future.result().result
    print("success:", result.success, "position:", result.position, result.message)
    node.destroy_node()
    rclpy.shutdown()


if __name__ == "__main__":
    main()
