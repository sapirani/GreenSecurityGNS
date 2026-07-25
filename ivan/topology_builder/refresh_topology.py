import argparse
import asyncio
import subprocess
import time

from ivan.topology_builder.consts import USERNAME, PASSWORD, GNS3_URL, PROJECT_ID, hadoop_nodes_replicas, \
    constant_nodes_replicas, hadoop_devices_position_config, constant_devices_position_config, IMAGES_TO_REFRESH
from ivan.topology_builder.topology_manager import TopologyManager


async def restart_entire_topology():
    async with TopologyManager(
            USERNAME, PASSWORD, GNS3_URL, PROJECT_ID, hadoop_nodes_replicas, constant_nodes_replicas) as topology_manager:
        await topology_manager.refresh_custom_images(IMAGES_TO_REFRESH)
        await topology_manager.restart_entire_topology(hadoop_devices_position_config, constant_devices_position_config)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--rebuild-images",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Rebuild all custom hadoop images"
    )

    parser.add_argument(
        "--refresh-socat",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Refresh the socat forwarder (default: enabled)."
    )

    args = parser.parse_args()
    should_refresh_socat = args.refresh_socat
    should_rebuild_images = args.rebuild_images

    if should_refresh_socat:
        # Ask for the sudo password once
        subprocess.run(["sudo", "-v"], check=True)


    if should_rebuild_images:
        print("Rebuilding custom hadoop images")
        TopologyManager.build_docker_images(IMAGES_TO_REFRESH)

    asyncio.run(restart_entire_topology())

    if should_refresh_socat:
        print("Killing the previous socat process...")
        time.sleep(10)   # waiting for the dhclient to receive a new ip
        subprocess.run(["sudo", "pkill", "-f", "TCP-LISTEN:8000,fork,reuseaddr"], check=True)
