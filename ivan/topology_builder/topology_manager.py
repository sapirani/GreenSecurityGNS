import asyncio
import subprocess
from asyncio import Future
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
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
    ):
        self.username = username
        self.password = password
        self.gns_url = gns_url
        self.project_id = project_id
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

    async def delete_template_task(self, template_id):
        async with self.session.delete(
                f"{self.gns_url}/templates/{template_id}") as response:
            response.raise_for_status()

    @staticmethod
    def ensure_no_exceptions(results: Future[Tuple]):
        for result in results:
            if isinstance(result, Exception):
                raise result

    async def delete_old_topology(self):
        print("Deleting old topology..")
        print("Fetching all nodes ids...")
        all_nodes_ids = await self.get_all_nodes_ids()

        print("Deleting all nodes..")
        delete_nodes_tasks = [self.delete_node(node_id) for node_id in all_nodes_ids]
        results = await asyncio.gather(*delete_nodes_tasks, return_exceptions=True)
        self.ensure_no_exceptions(results)

    async def get_template_names_to_ids(self):
        print("Fetching all template IDs..")
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

    def create_all_nodes_tasks(
            self,
            template_names_to_ids: Dict[str, str],
            hadoop_devices_position_config: DevicesPositionConfig,
            constant_devices_position_config: DevicesPositionConfig,
    ) -> List[Coroutine]:
        create_hadoop_nodes_tasks = self._create_nodes_tasks(
            template_names_to_ids,
            self.hadoop_nodes_replicas,
            hadoop_devices_position_config
        )

        create_constant_nodes_tasks = self._create_nodes_tasks(
            template_names_to_ids,
            self.constant_nodes_replicas,
            constant_devices_position_config
        )

        return [*create_hadoop_nodes_tasks, *create_constant_nodes_tasks]

    async def create_all_nodes(
            self,
            template_names_to_ids: Dict[str, str],
            hadoop_devices_position_config: DevicesPositionConfig,
            constant_devices_position_config: DevicesPositionConfig
    ) -> Dict[str, str]:
        creation_tasks = self.create_all_nodes_tasks(
            template_names_to_ids,
            hadoop_devices_position_config,
            constant_devices_position_config
        )
        created_nodes_results = await asyncio.gather(*creation_tasks)
        self.ensure_no_exceptions(created_nodes_results)
        print("Created all nodes")

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

        results = await asyncio.gather(*link_tasks)
        self.ensure_no_exceptions(results)
        print("Created all links")

    async def start_all_nodes(self):
        async with self.session.post(f"{self.gns_url}/projects/{self.project_id}/nodes/start") as response:
            response.raise_for_status()

    async def create_topology(
            self,
            hadoop_devices_position_config: DevicesPositionConfig,
            constant_devices_position_config: DevicesPositionConfig
    ):
        print("Starting creating topology...")
        print("Fetching all templates ids")
        template_names_to_ids = await self.get_template_names_to_ids()

        print("Creating all nodes..")
        node_name_to_id = await self.create_all_nodes(
            template_names_to_ids, hadoop_devices_position_config,
            constant_devices_position_config
        )

        print("Creating all links..")
        await self.create_all_links(node_name_to_id)


    async def restart_entire_topology(
            self,
            hadoop_devices_position_config: DevicesPositionConfig,
            constant_devices_position_config: DevicesPositionConfig
    ):
        await self.delete_old_topology()
        await self.create_topology(hadoop_devices_position_config, constant_devices_position_config)

        print("Staring project...")
        await self.start_all_nodes()
        print("Project started successfully.")

    async def refresh_custom_images(self, images_names: List[str]):
        template_names_to_ids = await self.get_template_names_to_ids()
        all_delete_tasks = [
            self.delete_template_task(template_names_to_ids[image_name])
            for image_name in images_names if image_name in template_names_to_ids
        ]
        print("Deleting images:", images_names)
        results = await asyncio.gather(*all_delete_tasks)
        self.ensure_no_exceptions(results)

        print("Creating images:", images_names)
        create_images_tasks = [self.create_template(image_name) for image_name in images_names]
        results = await asyncio.gather(*create_images_tasks)
        self.ensure_no_exceptions(results)

    async def create_template(self, template_name: str):
        templates_config = {
            "compute_id": "local",
            "name": template_name,
            "template_type": "docker",
            "image": f"{template_name}:latest",
        }

        async with self.session.post(f"{self.gns_url}/templates", json=templates_config) as response:
            response.raise_for_status()

    @staticmethod
    def build_docker_images(images_names: List[str]):
        subprocess.run(
            ["docker", "build", "-t", "hadoop-base-start", Path("ivan") / "base"],
            check=True,
        )

        subprocess.run(
            ["docker", "build", "-t", "hadoop-measurements-base", Path("ivan") / "measurements_base"],
            check=True,
        )

        subprocess.run(
            ["docker", "build", "-t", "hadoop-env", Path("ivan") / "environment_setup"],
            check=True,
        )

        def build_image(image_name: str):
            subprocess.run(
                ["docker", "build", "-t", image_name, Path("ivan") / image_name],
                check=True,
            )

        with ThreadPoolExecutor(max_workers=len(images_names)) as executor:
            list(executor.map(build_image, images_names))
