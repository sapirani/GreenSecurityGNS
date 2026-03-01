import docker
import time

from ivan.nodes_configuration_code.docker_stats.cpu_docker_stats import calculate_cpu_percent
from ivan.nodes_configuration_code.docker_stats.memory_docker_stats import get_memory_usage

client = docker.from_env()

while True:
    for container in client.containers.list():
        stats = container.stats(stream=False)
        cpu = calculate_cpu_percent(stats)
        memory_usage, memory_percent = get_memory_usage(stats)
        print(f"{container.name}: {cpu:.2f}% CPU, {memory_percent:.2f}% RAM, {memory_usage:.2f}GB RAM")
    time.sleep(2)
