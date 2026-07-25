import asyncio
from asyncio import Future
from typing import Tuple, Dict, List, Any
import aiohttp
from typing_extensions import Coroutine

from ivan.topology_builder.devices_position_config import DevicesPositionConfig


class TopologyManager:
    """Assuming only one switch and less than 8 hadoop devices (for now...)"""
    def __init__(
            self,
            username: str,
            password: str,
            gns_url: str,
            project_id: str,
            hadoop_nodes_replicas: Dict[str, int],
            constant_nodes_replicas: Dict[str, int],
            hadoop_devices_position_config: DevicesPositionConfig,
            constant_devices_position_config: DevicesPositionConfig,
    ):
        self.username = username
        self.password = password
        self.gns_url = gns_url
        self.project_id = project_id
        self.hadoop_devices_position_config = hadoop_devices_position_config
        self.constant_devices_position_config = constant_devices_position_config
        self.hadoop_nodes_replicas = hadoop_nodes_replicas
        self.constant_nodes_replicas = constant_nodes_replicas

        self.all_nodes_replicas = {**hadoop_nodes_replicas, **constant_nodes_replicas}

        self.auth = aiohttp.BasicAuth(username, password)
        self.session = aiohttp.ClientSession(auth=self.auth)

        # TODO: THINK ABOUT BETTER HANDLING
        self.switch_id = None

    async def __aenter__(self):
        await self.session.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.session.__aexit__(exc_type, exc_val, exc_tb)

    @staticmethod
    def get_hadoop_device_position(
            device_num: int,
            total_hadoop_devices: int,
            conf: DevicesPositionConfig,
    ) -> Tuple[int, int]:
        num_of_devices_in_row = int(total_hadoop_devices / conf.number_of_rows)

        if conf.number_of_rows == 1:
            y = int(conf.y_top)
        else:
            selected_row = (device_num // num_of_devices_in_row)
            y = int(conf.y_top) + int((conf.y_bottom - conf.y_top) * selected_row / (conf.number_of_rows - 1))

        index_in_row = (device_num % num_of_devices_in_row)
        x = int(conf.x_start) + int((conf.x_stop - conf.x_start) * index_in_row / (num_of_devices_in_row - 1))

        return x, y

    async def create_single_node(self, template_name: str, template_id: str, x: int, y: int):
        """Worker task to create an individual node."""
        node_config = {
            "compute_id": "local",
            "x": x,
            "y": y,
        }

        url = f"{self.gns_url}/projects/{self.project_id}/templates/{template_id}"
        async with self.session.post(url, json=node_config) as response:
            response.raise_for_status()
            created_node = await response.json()

            if "Ethernet switch" == template_name:
                self.switch_id = created_node["node_id"]

            return created_node

    async def get_all_nodes_ids(self) -> List[str]:
        async with self.session.get(f"{self.gns_url}/projects/{self.project_id}/nodes") as response:
            response.raise_for_status()
            all_nodes = await response.json()
            return [node["node_id"] for node in all_nodes]

    async def delete_node(self, node_id: str):
        async with self.session.delete(f"{self.gns_url}/projects/{self.project_id}/nodes/{node_id}") as response:
            response.raise_for_status()

    @staticmethod
    def ensure_all_nodes_deleted(all_nodes_ids: List[str], results: Future[Tuple]):
        for node_id, result in zip(all_nodes_ids, results):
            if isinstance(result, Exception):
                print(f"Failed to delete node {node_id}: {result}")
                raise RuntimeError(f"Failed to delete node {node_id}: {result}")

    async def delete_old_topology(self):
        print("Deleting old topology..")
        print("Fetching all nodes ids...")
        all_nodes_ids = await self.get_all_nodes_ids()

        print("Deleting all nodes..")
        delete_nodes_tasks = [self.delete_node(node_id) for node_id in all_nodes_ids]
        results = await asyncio.gather(*delete_nodes_tasks, return_exceptions=True)
        self.ensure_all_nodes_deleted(all_nodes_ids, results)

    async def get_template_names_to_ids(self):
        async with self.session.get(f"{self.gns_url}/templates") as response:
            response.raise_for_status()
            templates = await response.json()

        return {
            template["name"]: template["template_id"]
            for template in templates if template["name"] in self.all_nodes_replicas
        }

    @staticmethod
    def _flatten_dict_of_counts(dict_of_counts):
        return [
            node
            for node, replicas in dict_of_counts.items()
            for _ in range(replicas)
        ]
    
    def _create_nodes_tasks(
            self,
            template_names_to_ids: Dict[str, str],
            nodes_replicas: Dict[str, int],
            devices_position_config: DevicesPositionConfig
    ) -> List[Coroutine]:
        creation_tasks = []

        flattened_templates = self._flatten_dict_of_counts(nodes_replicas)
        for device_num, template_name in enumerate(flattened_templates):
            template_id = template_names_to_ids[template_name]
            x, y = self.get_hadoop_device_position(
                device_num,
                len(flattened_templates),
                devices_position_config
            )
            creation_tasks.append(self.create_single_node(template_name, template_id, x, y))

        return creation_tasks

    def create_all_nodes_tasks(self, template_names_to_ids: Dict[str, str]) -> List[Coroutine]:
        create_hadoop_nodes_tasks = self._create_nodes_tasks(
            template_names_to_ids,
            self.hadoop_nodes_replicas,
            self.hadoop_devices_position_config
        )

        create_constant_nodes_tasks = self._create_nodes_tasks(
            template_names_to_ids,
            self.constant_nodes_replicas,
            self.constant_devices_position_config
        )

        return [*create_hadoop_nodes_tasks, *create_constant_nodes_tasks]

    async def create_all_nodes(self, template_names_to_ids: Dict[str, str]) -> Dict[str, str]:
        creation_tasks = self.create_all_nodes_tasks(template_names_to_ids)
        created_nodes_results = await asyncio.gather(*creation_tasks)
        print("Created all nodes")

        # Process results sequentially to extract IDs
        return {node["name"]: node["node_id"] for node in created_nodes_results}

    async def create_link(self, link_conf: Dict[str, Any]):
        url = f"{self.gns_url}/projects/{self.project_id}/links"
        async with self.session.post(url, json=link_conf) as resp:
            await resp.json()
            resp.raise_for_status()

    async def create_all_links(self, node_name_to_id):
        link_tasks = []
        for switch_port, node_id in enumerate(node_name_to_id.values()):
            if node_id == self.switch_id:
                continue

            link_config = {
                "nodes": [
                    {"node_id": node_id, "port_number": 0, "adapter_number": 0},
                    {"node_id": self.switch_id, "port_number": switch_port, "adapter_number": 0}
                ]
            }

            link_tasks.append(self.create_link(link_config))

        # Execute all link commands concurrently
        await asyncio.gather(*link_tasks)
        print("Created all links")

    async def start_all_nodes(self):
        async with self.session.post(f"{self.gns_url}/projects/{self.project_id}/nodes/start") as response:
            response.raise_for_status()

    async def create_topology(self):
        print("Starting creating topology...")
        print("Fetching all templates ids")
        template_names_to_ids = await self.get_template_names_to_ids()

        print("Creating all nodes..")
        node_name_to_id = await self.create_all_nodes(template_names_to_ids)

        print("Creating all links..")
        await self.create_all_links(node_name_to_id)


    async def restart_entire_topology(self):
        await self.delete_old_topology()
        await self.create_topology()

        print("Staring project...")
        await self.start_all_nodes()
        print("Project started successfully.")

