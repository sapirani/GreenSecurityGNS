import argparse
import re
from enum import Enum


class CompressionCodec(Enum):
    DEFAULT = "org.apache.hadoop.io.compress.DefaultCodec"
    SNAPPY = "org.apache.hadoop.io.compress.SnappyCodec"
    GZIP = "org.apache.hadoop.io.compress.GzipCodec"
    LZO = "com.hadoop.compression.lzo.LzoCodec"
    BZIP2 = "org.apache.hadoop.io.compress.BZip2Codec"
    LZ4 = "org.apache.hadoop.io.compress.Lz4Codec"


def positive_int(value: str) -> int:
    try:
        int_value = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid integer value: {value}")

    if int_value <= 0:
        raise argparse.ArgumentTypeError(f"Value should be a positive integer: {value}")
    return int_value


def codec_type(value: str) -> CompressionCodec:
    """Validate and convert string to CompressionCodec enum."""
    try:
        value_upper = value.upper()
        for codec in CompressionCodec:
            if value_upper == codec.name or value == codec.value:
                return codec
        raise ValueError()
    except ValueError:
        valid_options = [f"{c.name} ('{c.value}')" for c in CompressionCodec]
        raise argparse.ArgumentTypeError(
            f"Invalid codec '{value}'. Valid options: {', '.join(valid_options)}"
        )


def parse_size(value: str) -> int:
    """
    Parse a size string with optional units: B, KB, MB, GB (case-insensitive).
    Returns the size in bytes as an int.

    Examples:
    - "128MB" -> 134217728
    - "1G"    -> 1073741824
    - "512kb" -> 524288
    - "100"   -> 100 bytes
    """
    units = {
        "B": 1,
        "KB": 1024,
        "K": 1024,
        "MB": 1024 ** 2,
        "M": 1024 ** 2,
        "GB": 1024 ** 3,
        "G": 1024 ** 3
    }

    pattern = r"^\s*(\d+)\s*([KMGB]{1,2})?\s*$"
    match = re.match(pattern, value.strip().upper())

    if not match:
        raise argparse.ArgumentTypeError(f"Invalid size value: '{value}'")

    number, unit = match.groups()
    number = int(number)
    multiplier = units.get(unit, 1)

    return number * multiplier


def run_distributed_task(arguments):
    job_str = f"""
hadoop jar /opt/hadoop-3.4.1/share/hadoop/tools/lib/hadoop-streaming-*.jar
  -input hdfs://namenode-1:9000{arguments.input_path}
  -output hdfs://namenode-1:9000{arguments.output_path}
  -mapper {arguments.mapper_path}
  -reducer {arguments.reducer_path}
  -file {arguments.mapper_path}
  -file {arguments.reducer_path}
  -D mapreduce.job.maps={arguments.mappers}
  -D mapreduce.job.reduces={arguments.reducers}
  -D mapreduce.map.memory.mb={arguments.map_memory}
  -D mapreduce.reduce.memory.mb={arguments.reduce_memory}
  -D yarn.app.mapreduce.am.resource.mb={arguments.application_manager_memory}
  -D mapreduce.map.cpu.vcores={arguments.map_vcores}
  -D mapreduce.reduce.cpu.vcores={arguments.reduce_vcores}
  -D yarn.app.mapreduce.am.resource.cpu-vcores={arguments.application_manager_vcores}
  -D mapreduce.task.io.sort.mb={arguments.sort_buffer_mb}
  -D mapreduce.task.io.sort.factor={arguments.io_sort_factor}
  -D mapreduce.map.output.compress={str(arguments.should_compress).lower()}
  -D mapreduce.map.output.compress.codec={arguments.map_compress_codec.value}
  -D mapreduce.input.fileinputformat.split.maxsize={arguments.max_split_size}
  -D mapreduce.input.fileinputformat.split.minsize={arguments.min_split_size}
  -D mapreduce.reduce.shuffle.parallelcopies={arguments.shuffle_copies}
  -D mapreduce.job.jvm.numtasks={arguments.jvm_numtasks}
  -D mapreduce.job.reduce.slowstart.completedmaps={arguments.slowstart_completed_maps}
"""

    print(job_str)


# TODO: subprocess.run ...


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="This program serves as a wrapper for starting a distributed task using Hadoop framework"
    )

    task_definition_group = parser.add_argument_group("Task Definition Settings")

    task_definition_group.add_argument(
        "-i", "--input_path",
        type=str,
        default="/input",
        help="A path to the input file of the distributed task"
    )

    task_definition_group.add_argument(
        "-o", "--output_path",
        type=str,
        default="/output",
        help="A path to the output file of the distributed task"
    )

    task_definition_group.add_argument(
        "-mp", "--mapper_path",
        type=str,
        default="/home/mapper.py",
        help="A path to a Python mapper implementation file"
    )

    task_definition_group.add_argument(
        "-rp", "--reducer_path",
        type=str,
        default="/home/mapper.py",
        help="A path to a Python reducer implementation file"
    )

    parallelism_and_scheduling_group = parser.add_argument_group("Parallelism & Scheduling Settings")

    parallelism_and_scheduling_group.add_argument(
        "-m", "--mappers",
        type=positive_int,
        default=2,
        help="Number of mappers"
    )

    parallelism_and_scheduling_group.add_argument(
        "-r", "--reducers",
        type=positive_int,
        default=1,
        help="Number of reducers"
    )

    parallelism_and_scheduling_group.add_argument(
        "-mc", "--map_vcores",
        type=positive_int,
        default=1,
        help="Number of virtual CPU cores per map task. "
    )

    parallelism_and_scheduling_group.add_argument(
        "-rc", "--reduce_vcores",
        type=positive_int,
        default=1,
        help="Number of vcores to allocate for each reduce task"
    )

    parallelism_and_scheduling_group.add_argument(
        "-ac", "--application_manager_vcores",
        type=positive_int,
        default=1,
        help="Number of vcores to allocate to the application manager"
    )

    parallelism_and_scheduling_group.add_argument(
        "-sc", "--shuffle_copies",
        type=positive_int,
        default=5,
        help="Number of parallel fetches per reduce task during shuffle. "
             "More copies speed up shuffle but risk saturating network or disk I/O."
    )

    parallelism_and_scheduling_group.add_argument(
        "-jvm", "--jvm_numtasks",
        type=int,
        default=1,
        help="Number of tasks per JVM to reduce JVM startup overhead. Default: 1"
    )

    parallelism_and_scheduling_group.add_argument(
        "-ssc", "--slowstart_completed_maps",
        type=float,
        default=0.05,
        help="Fraction of map tasks to complete before starting reduce tasks. "
             "Higher delays reduce phase but reduces load on shuffle. Default: 0.05"
    )

    memory_group = parser.add_argument_group("Memory Settings")

    memory_group.add_argument(
        "-mm", "--map_memory",
        type=positive_int,
        default=1024,
        help="Memory allocated per map task (MB). "
             "Higher memory can improve performance for large tasks but reduces parallelism if cluster RAM is limited."

    )

    memory_group.add_argument(
        "-rm", "--reduce_memory",
        type=positive_int,
        default=1024,
        help="Memory allocation size for each reduce task in MB"
    )

    memory_group.add_argument(
        "-am", "--application_manager_memory",
        type=positive_int,
        default=1536,
        help="Memory allocation size for the application manager in MB"
    )

    memory_group.add_argument(
        "-sb", "--sort_buffer_mb",
        type=positive_int,
        default=100,
        help="Memory allocation amount for a buffer used for sorting. "
             "Represents a trade-off between memory and disk I/O operations."
    )

    memory_group.add_argument(
        "--min_split_size",
        type=parse_size,
        default="0B",
        help="Minimum input split size (e.g., 64MB). "
             "Larger min split size reduces the number of map tasks, "
             "improving startup overhead but may reduce parallelism."
    )

    memory_group.add_argument(
        "--max_split_size",
        type=parse_size,
        default="128MB",
        help="Maximum input split size with units (B, KB, MB, GB). "
             "Effectively determines the number of mappers that will be used (together with the input size)"
    )

    shuffle_and_compression_group = parser.add_argument_group("Shuffle & Compression Settings")

    shuffle_and_compression_group.add_argument(
        "--io_sort_factor",
        type=positive_int,
        default=10,
        help="Number of streams merged simultaneously during map output sort."
    )

    shuffle_and_compression_group.add_argument(
        "-c", "--should_compress",
        action="store_true",
        default=False,
        help="Enable compression of map outputs before shuffle. "
             "Compression reduces network traffic at the cost of additional CPU usage."
    )

    shuffle_and_compression_group.add_argument(
        "-mcc", "--map_compress_codec",
        type=codec_type,
        default=CompressionCodec.DEFAULT,
        help="Compression codec for map output. "
             "Options: " + ", ".join(f"{c.name} ('{c.value}')" for c in CompressionCodec)
    )

    args = parser.parse_args()
    run_distributed_task(args)
