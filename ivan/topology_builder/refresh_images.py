import asyncio

from ivan.topology_builder.consts import USERNAME, PASSWORD, GNS3_URL, PROJECT_ID, hadoop_nodes_replicas, \
    IMAGES_TO_REFRESH, constant_nodes_replicas
from ivan.topology_builder.topology_manager import TopologyManager


async def refresh_custom_images():
    async with TopologyManager(
            USERNAME, PASSWORD, GNS3_URL, PROJECT_ID, hadoop_nodes_replicas, constant_nodes_replicas) as topology_manager:
        await topology_manager.refresh_custom_images(IMAGES_TO_REFRESH)


if __name__ == "__main__":
    asyncio.run(refresh_custom_images())