import docker
import time

from ivan.nodes_configuration_code.docker_stats.cpu_docker_stats import calculate_cpu_percent

client = docker.from_env()

while True:
    for container in client.containers.list():
        stats = container.stats(stream=False)
        cpu = calculate_cpu_percent(stats)
        memory = calculate_memory_usage(stats)
        print(f"{container.name}: {cpu:.2f}% CPU, {memory:.2f}%")
    time.sleep(2)
