class DockerMemoryConsts:
    MEMORY_STATS = "memory_stats"
    USAGE = "usage"
    MAX_USAGE = "max_usage"
    LIMIT = "limit"

def get_memory_usage(stat: dict) -> tuple[float, float]:
    """
    Returns a dictionary with current memory usage, max usage, and memory limit in bytes.
    """
    memory_stats = stat.get(DockerMemoryConsts.MEMORY_STATS, {})
    memory_usage = memory_stats.get(DockerMemoryConsts.USAGE, 0)
    memory_limit = memory_stats.get(DockerMemoryConsts.LIMIT, 1)
    memory_percent = memory_usage / memory_limit * 100
    return memory_usage, memory_percent