class DockerCPUConsts:
    CPU_STATS = "cpu_stats"
    CPU_USAGE = "cpu_usage"
    TOTAL_USAGE = "total_usage"
    SYSTEM_CPU_USAGE = "system_cpu_usage"
    PRECPU_STATS = "precpu_stats"
    PRECPU_USAGE = "precpu_usage"


def calculate_cpu_percent(stat: dict) -> float:
    cpu_total_usage = stat[DockerCPUConsts.CPU_STATS][DockerCPUConsts.CPU_USAGE][DockerCPUConsts.TOTAL_USAGE]
    precpu_total_usage = stat[DockerCPUConsts.PRECPU_STATS][DockerCPUConsts.CPU_USAGE][DockerCPUConsts.TOTAL_USAGE]
    cpu_delta = cpu_total_usage - precpu_total_usage

    system_cpu_usage = stat[DockerCPUConsts.CPU_STATS][DockerCPUConsts.SYSTEM_CPU_USAGE]
    precpu_system_cpu_usage = stat[DockerCPUConsts.PRECPU_STATS][DockerCPUConsts.SYSTEM_CPU_USAGE]
    system_cpu_delta = system_cpu_usage - precpu_system_cpu_usage

    cpu_count = len(stat[DockerCPUConsts.CPU_STATS][DockerCPUConsts.CPU_USAGE].get(DockerCPUConsts.PRECPU_USAGE, [])) or 1
    if system_cpu_delta > 0 and cpu_delta > 0:
        return (cpu_delta / system_cpu_delta) * cpu_count * 100.0
    return 0.0
