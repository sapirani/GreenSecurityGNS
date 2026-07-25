import argparse
import asyncio
import aiohttp
import subprocess
import yaml

with open("config.yaml") as f:
    config = yaml.safe_load(f)
    USERNAME = config["username"]
    PASSWORD = config["password"]

GNS3_URL = "http://192.168.140.110:3080/v2"
PROJECT_ID = "8b1df7a1-d0ea-4bd9-a5d7-f0c6bb61b40b"

hadoop_nodes_replicas = {
    "resourcemanager": 1,
    "namenode": 1,
    "datanode": 3,
    "historyserver": 1,
}

constant_nodes_replicas = {
    "NAT": 1,
    "Ethernet switch": 1,
}

X_START = -300
X_STOP = 300

switch_id = None

all_nodes_replicas = {**hadoop_nodes_replicas, **constant_nodes_replicas}
total_hadoop_devices = sum(hadoop_nodes_replicas.values())


async def create_single_node(session, auth, template_name, template_id, device_num):
    """Worker task to create an individual node."""
    global switch_id

    node_config = {
        "compute_id": "local",
        "x": X_START + int((X_STOP - X_START) * (device_num / total_hadoop_devices)),
        "y": 0,
    }

    url = f"{GNS3_URL}/projects/{PROJECT_ID}/templates/{template_id}"
    async with session.post(url, json=node_config, auth=auth) as response:
        response.raise_for_status()
        created_node = await response.json()

        if "Ethernet switch" == template_name:  # Handles potential name increments like Ethernet switch-1
            switch_id = created_node["node_id"]

        return created_node

async def get_all_nodes_ids(session, auth):
    async with session.get(f"{GNS3_URL}/projects/{PROJECT_ID}/nodes", auth=auth) as response:
        response.raise_for_status()
        all_nodes = await response.json()
        return [node["node_id"] for node in all_nodes]

async def delete_node(session, auth, node_id: str):
    async with session.delete(f"{GNS3_URL}/projects/{PROJECT_ID}/nodes/{node_id}", auth=auth) as response:
        response.raise_for_status()

async def main():
    auth = aiohttp.BasicAuth(USERNAME, PASSWORD)

    async with aiohttp.ClientSession() as session:
        print("Fetching all nodes ids...")
        all_nodes_ids = await get_all_nodes_ids(session, auth)

        delete_nodes_tasks = [delete_node(session, auth, node_id) for node_id in all_nodes_ids]
        print("Deleting all nodes..")
        results = await asyncio.gather(*delete_nodes_tasks, return_exceptions=True)

        for node_id, result in zip(all_nodes_ids, results):
            if isinstance(result, Exception):
                print(f"Failed to delete node {node_id}: {result}")

        print("Starting creating topology...")
        # 1. Fetch all templates asynchronously
        async with session.get(f"{GNS3_URL}/templates", auth=auth) as response:
            response.raise_for_status()
            templates = await response.json()

        template_name_to_id = {
            template["name"]: template["template_id"]
            for template in templates if template["name"] in all_nodes_replicas
        }

        # 2. Build the task list for concurrent node creation
        creation_tasks = []
        device_num = 0

        for template_name, count in all_nodes_replicas.items():
            template_id = template_name_to_id[template_name]
            for _ in range(count):
                task = create_single_node(session, auth, template_name, template_id, device_num)
                creation_tasks.append(task)
                device_num += 1

        # Fire all node creation requests simultaneously
        created_nodes_results = await asyncio.gather(*creation_tasks)
        print("Created all nodes")

        # Process results sequentially to extract IDs
        node_name_to_id = {}
        for node in created_nodes_results:
            node_name_to_id[node["name"]] = node["node_id"]

        # 3. Create all links concurrently
        link_tasks = []
        for switch_port, node_id in enumerate(node_name_to_id.values()):
            if node_id == switch_id:
                continue

            link_config = {
                "nodes": [
                    {"node_id": node_id, "port_number": 0, "adapter_number": 0},
                    {"node_id": switch_id, "port_number": switch_port, "adapter_number": 0}
                ]
            }

            async def post_link(link_conf):
                url = f"{GNS3_URL}/projects/{PROJECT_ID}/links"
                async with session.post(url, json=link_conf, auth=auth) as resp:
                    await resp.json()
                    resp.raise_for_status()

            link_tasks.append(post_link(link_config))

        # Execute all link commands concurrently
        await asyncio.gather(*link_tasks)
        print("Created all links")

        # 4. Start all project nodes
        async with session.post(f"{GNS3_URL}/projects/{PROJECT_ID}/nodes/start", auth=auth) as response:
            response.raise_for_status()
            print("All nodes started successfully.")


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

    asyncio.run(main())

    if should_refresh_socat:
        print("Killing the previous socat process...")
        subprocess.run(["sudo", "pkill", "-f", "TCP-LISTEN:8000,fork,reuseaddr"], check=True)
