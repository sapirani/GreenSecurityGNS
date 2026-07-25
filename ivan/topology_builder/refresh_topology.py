import argparse
import asyncio
import os
import subprocess
from pathlib import Path

import yaml

from ivan.topology_builder.devices_position_config import DevicesPositionConfig
from ivan.topology_builder.topology_manager import TopologyManager

with open(Path("topology_builder") / "config.yaml") as f:
    config = yaml.safe_load(f)
    USERNAME = config["username"]
    PASSWORD = config["password"]

GNS3_URL = "http://192.168.140.110:3080/v2"
PROJECT_ID = "8b1df7a1-d0ea-4bd9-a5d7-f0c6bb61b40b"

hadoop_nodes_replicas = {
    "resourcemanager": 1,
    "namenode": 1,
    "historyserver": 1,
    "datanode": 3,
}

constant_nodes_replicas = {
    "Ethernet switch": 1,
    "NAT": 1,
}

hadoop_devices_position_config = DevicesPositionConfig(
    x_start=-200,
    x_stop=200,
    y_top=-100,
    y_bottom=100,
    number_of_rows=2,
)

constant_devices_position_config = DevicesPositionConfig(
    x_start=-0,
    x_stop=100,
    y_top=-0,
    y_bottom=0,
    number_of_rows=1,
)

# all_nodes_replicas = {**hadoop_nodes_replicas, **constant_nodes_replicas}
#
#
# def get_hadoop_device_position(device_num: int, total_hadoop_devices: int, x_start, x_stop, y_top, y_bottom, number_of_rows) -> Tuple[float, float]:
#     num_of_devices_in_row = int(total_hadoop_devices / number_of_rows)
#
#     if number_of_rows == 1:
#         y = y_top
#     else:
#         y = y_top + int((y_bottom - y_top) * (device_num // num_of_devices_in_row) / (number_of_rows - 1))
#
#     x = x_start + int((x_stop - x_start) * (device_num % num_of_devices_in_row) / (num_of_devices_in_row - 1))
#
#     return x, y
#
#
# async def create_single_node(session, auth, template_name, template_id, x, y):
#     """Worker task to create an individual node."""
#     global switch_id
#
#     node_config = {
#         "compute_id": "local",
#         "x": x,
#         "y": y,
#     }
#
#     url = f"{GNS3_URL}/projects/{PROJECT_ID}/templates/{template_id}"
#     async with session.post(url, json=node_config, auth=auth) as response:
#         response.raise_for_status()
#         created_node = await response.json()
#
#         if "Ethernet switch" == template_name:  # Handles potential name increments like Ethernet switch-1
#             switch_id = created_node["node_id"]
#
#         return created_node
#
# async def get_all_nodes_ids(session, auth):
#     async with session.get(f"{GNS3_URL}/projects/{PROJECT_ID}/nodes", auth=auth) as response:
#         response.raise_for_status()
#         all_nodes = await response.json()
#         return [node["node_id"] for node in all_nodes]
#
# async def delete_node(session, auth, node_id: str):
#     async with session.delete(f"{GNS3_URL}/projects/{PROJECT_ID}/nodes/{node_id}", auth=auth) as response:
#         response.raise_for_status()
#
# def ensure_all_nodes_deleted(all_nodes_ids, results):
#     for node_id, result in zip(all_nodes_ids, results):
#         if isinstance(result, Exception):
#             print(f"Failed to delete node {node_id}: {result}")
#             raise RuntimeError(f"Failed to delete node {node_id}: {result}")
#
#
# async def delete_old_topology(session, auth):
#     print("Deleting old topology..")
#     print("Fetching all nodes ids...")
#     all_nodes_ids = await get_all_nodes_ids(session, auth)
#
#     print("Deleting all nodes..")
#     delete_nodes_tasks = [delete_node(session, auth, node_id) for node_id in all_nodes_ids]
#     results = await asyncio.gather(*delete_nodes_tasks, return_exceptions=True)
#     ensure_all_nodes_deleted(all_nodes_ids, results)
#
#
# async def get_template_names_to_ids(session, auth):
#     async with session.get(f"{GNS3_URL}/templates", auth=auth) as response:
#         response.raise_for_status()
#         templates = await response.json()
#
#     return {
#         template["name"]: template["template_id"]
#         for template in templates if template["name"] in all_nodes_replicas
#     }
#
# def _flatten_dict_of_counts(dict_of_counts):
#     return [
#         node
#         for node, replicas in dict_of_counts.items()
#         for _ in range(replicas)
#     ]
#
# def create_all_nodes_tasks(session, auth, template_names_to_ids):
#     creation_tasks = []
#
#     flattened_hadoop_templates = _flatten_dict_of_counts(hadoop_nodes_replicas)
#     for hadoop_device_num, template_name in enumerate(flattened_hadoop_templates):
#         template_id = template_names_to_ids[template_name]
#         x, y = get_hadoop_device_position(hadoop_device_num, len(flattened_hadoop_templates), HADOOP_DEVICE_X_START, HADOOP_DEVICE_X_STOP, HADOOP_DEVICE_Y_TOP, HADOOP_DEVICE_Y_BOTTOM, 2)
#         creation_tasks.append(create_single_node(session, auth, template_name, template_id, x, y))
#
#     flattened_constant_templates = _flatten_dict_of_counts(constant_nodes_replicas)
#     for constant_device_num, template_name in enumerate(flattened_constant_templates):
#         template_id = template_names_to_ids[template_name]
#         x, y = get_hadoop_device_position(constant_device_num, len(flattened_constant_templates), CONSTANT_DEVICE_X_START, CONSTANT_DEVICE_X_STOP, CONSTANT_DEVICE_Y_TOP, CONSTANT_DEVICE_Y_TOP, 1)
#         creation_tasks.append(create_single_node(session, auth, template_name, template_id, x, y))
#
#     return creation_tasks
#
#
# async def create_all_nodes(session, auth, template_names_to_ids):
#     creation_tasks = create_all_nodes_tasks(session, auth, template_names_to_ids)
#     created_nodes_results = await asyncio.gather(*creation_tasks)
#     print("Created all nodes")
#
#     # Process results sequentially to extract IDs
#     return {node["name"]: node["node_id"] for node in created_nodes_results}
#
#
# async def create_all_links(session, auth, node_name_to_id):
#     link_tasks = []
#     for switch_port, node_id in enumerate(node_name_to_id.values()):
#         if node_id == switch_id:
#             continue
#
#         link_config = {
#             "nodes": [
#                 {"node_id": node_id, "port_number": 0, "adapter_number": 0},
#                 {"node_id": switch_id, "port_number": switch_port, "adapter_number": 0}
#             ]
#         }
#
#         async def post_link(link_conf):
#             url = f"{GNS3_URL}/projects/{PROJECT_ID}/links"
#             async with session.post(url, json=link_conf, auth=auth) as resp:
#                 await resp.json()
#                 resp.raise_for_status()
#
#         link_tasks.append(post_link(link_config))
#
#     # Execute all link commands concurrently
#     await asyncio.gather(*link_tasks)
#     print("Created all links")
#
#
# async def start_project(session, auth):
#     async with session.post(f"{GNS3_URL}/projects/{PROJECT_ID}/nodes/start", auth=auth) as response:
#         response.raise_for_status()
#
#
# async def create_topology(session, auth):
#     print("Starting creating topology...")
#     print("Fetching all templates ids")
#     template_names_to_ids = await get_template_names_to_ids(session, auth)
#
#     print("Creating all nodes..")
#     node_name_to_id = await create_all_nodes(session, auth, template_names_to_ids)
#
#     print("Creating all links..")
#     await create_all_links(session, auth, node_name_to_id)
#
#     print("Staring project successfully.")
#     await start_project(session, auth)
#     print("Project started successfully.")

async def restart_entire_topology():
    async with TopologyManager(
            USERNAME, PASSWORD, GNS3_URL, PROJECT_ID, hadoop_nodes_replicas, constant_nodes_replicas,
            hadoop_devices_position_config, constant_devices_position_config) as topology_manager:
        await topology_manager.restart_entire_topology()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--refresh",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Refresh the socat forwarder (default: enabled)."
    )

    args = parser.parse_args()
    should_refresh_socat = args.refresh

    if should_refresh_socat:
        # Ask for the sudo password once
        subprocess.run(["sudo", "-v"], check=True)

    asyncio.run(restart_entire_topology())

    if should_refresh_socat:
        print("Killing the previous socat process...")
        subprocess.run(["sudo", "pkill", "-f", "TCP-LISTEN:8000,fork,reuseaddr"], check=True)
