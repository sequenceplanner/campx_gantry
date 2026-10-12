#!/bin/bash
set -e

# Both binaries link against the rcl/rmw shared libraries (r2r is a crate
# dependency), and `ros` also needs the gantry_interfaces typesupport, so the
# ROS underlay and the overlay have to be sourced before either can start.
source "/opt/ros/$ROS_DISTRO/setup.bash"
source /opt/gantry_interfaces/setup.bash

exec "$@"
