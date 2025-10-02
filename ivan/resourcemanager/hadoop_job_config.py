import argparse
import shlex
from enum import Enum
from typing import List
import re

from pydantic import BaseModel, Field

GENERAL_GROUP = "General"
HUMAN_READABLE_KEY = "human_readable"

units = {
    "B": 1,
    "KB": 1024,
    "K": 1024,
    "MB": 1024 ** 2,
    "M": 1024 ** 2,
    "GB": 1024 ** 3,
    "G": 1024 ** 3
}


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

    pattern = r"^\s*(\d+)\s*([KMGB]{1,2})?\s*$"
    match = re.match(pattern, value.strip().upper())

    if not match:
        raise argparse.ArgumentTypeError(f"Invalid size value: '{value}'")

    number, unit = match.groups()
    number = int(number)
    multiplier = units.get(unit, 1)

    return number * multiplier


class CompressionCodec(str, Enum):
    DEFAULT = "org.apache.hadoop.io.compress.DefaultCodec"
    SNAPPY = "org.apache.hadoop.io.compress.SnappyCodec"
    GZIP = "org.apache.hadoop.io.compress.GzipCodec"
    LZO = "com.hadoop.compression.lzo.LzoCodec"
    BZIP2 = "org.apache.hadoop.io.compress.BZip2Codec"
    LZ4 = "org.apache.hadoop.io.compress.Lz4Codec"


class Groups(str, Enum):
    TASK_DEFINITION = "Task Definition Settings"
    PARALLELISM_AND_SCHEDULING = "Parallelism & Scheduling Settings"
    MEMORY = "Memory Settings"
    SHUFFLE_AND_COMPRESSION = "Shuffle & Compression Settings"


class HadoopJobConfig(BaseModel):
    # Task Definition
    input_path: str = Field(
        default="/input",
        alias="i",
        title=Groups.TASK_DEFINITION.value,
        description="HDFS path to the input directory"
    )

    output_path: str = Field(
        default="/output",
        alias="o",
        title=Groups.TASK_DEFINITION.value,
        description="HDFS path to the output directory",
    )

    mapper_path: str = Field(
        default="/home/mapper.py",
        alias="mp",
        title=Groups.TASK_DEFINITION.value,
        description="Path to the mapper implementation",
    )

    reducer_path: str = Field(
        default="/home/reducer.py",
        alias="rp",
        title=Groups.TASK_DEFINITION.value,
        description="Path to the reducer implementation",
    )

    # Parallelism & Scheduling
    number_of_mappers: int = Field(
        default=2,
        gt=0,
        alias="m",
        title=Groups.PARALLELISM_AND_SCHEDULING.value,
        description="Number of mapper tasks",
    )

    number_of_reducers: int = Field(
        default=1,
        gt=0,
        alias="r",
        title=Groups.PARALLELISM_AND_SCHEDULING.value,
        description="Number of reducer tasks",
    )

    map_vcores: int = Field(
        default=1,
        gt=0,
        alias="mc",
        title=Groups.PARALLELISM_AND_SCHEDULING.value,
        description="Number of vCores per map task",
    )

    reduce_vcores: int = Field(
        default=1,
        gt=0,
        alias="rc",
        title=Groups.PARALLELISM_AND_SCHEDULING.value,
        description="Number of vCores per reduce task",
    )

    application_manager_vcores: int = Field(
        default=1,
        gt=0,
        alias="ac",
        title=Groups.PARALLELISM_AND_SCHEDULING.value,
        description="Number of vCores for the application master",
    )

    shuffle_copies: int = Field(
        default=5,
        gt=0,
        alias="sc",
        title=Groups.PARALLELISM_AND_SCHEDULING.value,
        description="Parallel copies per reduce during shuffle. "
                    "More copies speed up shuffle but risk saturating network or disk I/O.",
    )

    jvm_numtasks: int = Field(
        default=1,
        gt=0,
        alias="jvm",
        title=Groups.PARALLELISM_AND_SCHEDULING.value,
        description="Number of tasks per JVM to reduce JVM startup overhead.",
    )

    slowstart_completed_maps: float = Field(
        default=0.05,
        ge=0,
        alias="ssc",
        title=Groups.PARALLELISM_AND_SCHEDULING.value,
        description="Fraction of maps to finish before reduce begins. "
                    "Higher delays reduce phase but reduces load on shuffle.",
    )

    # Memory
    map_memory_mb: int = Field(
        default=1024,
        gt=0,
        alias="mm",
        title=Groups.MEMORY.value,
        description="Memory per map task (MB).",
    )

    reduce_memory_mb: int = Field(
        default=1024,
        gt=0,
        alias="rm",
        title=Groups.MEMORY.value,
        description="Memory per reduce task (MB)",
    )

    application_manager_memory_mb: int = Field(
        default=1536,
        gt=0,
        alias="am",
        title=Groups.MEMORY.value,
        description="Memory for application master (MB)",
    )

    sort_buffer_mb: int = Field(
        default=100,
        gt=0,
        alias="sb",
        title=Groups.MEMORY.value,
        description="Sort buffer size (MB)",
    )

    min_split_size: int = Field(
        default="0B",
        ge=0,
        alias="n",
        title=Groups.MEMORY.value,
        description="Minimum input split size with human-readable units (B, KB, MB, GB). "
                    "Larger min split size reduces the number of map tasks, "
                    "improving startup overhead but may reduce parallelism.",
        json_schema_extra={
            HUMAN_READABLE_KEY: True,
        },
    )

    max_split_size: int = Field(
        default=128 * 1024 * 1024,
        gt=0,
        alias="x",
        title=Groups.MEMORY.value,
        description="Maximum input split size with human-readable units (B, KB, MB, GB). "
                    "Effectively determines the number of mappers that will be used "
                    "(together with the input size).",
        json_schema_extra={
            HUMAN_READABLE_KEY: True
        },
    )

    # Shuffle & Compression
    io_sort_factor: int = Field(
        default=10,
        gt=0,
        alias="f",
        title=Groups.SHUFFLE_AND_COMPRESSION.value,
        description="Number of streams merged simultaneously during map output sort.",
    )

    should_compress: bool = Field(
        default=False,
        alias="c",
        title=Groups.SHUFFLE_AND_COMPRESSION.value,
        description="Enable compression of map outputs before shuffle. "
                    "Compression reduces network traffic at the cost of additional CPU usage.",
    )

    map_compress_codec: CompressionCodec = Field(
        default=CompressionCodec.DEFAULT,
        alias="mcc",
        title=Groups.SHUFFLE_AND_COMPRESSION.value,
        description="Compression codec for map output. "
                    "Options: " + ", ".join(f"{c.name} ('{c.value}')" for c in CompressionCodec)
    )

    @classmethod
    def from_argparse(cls, args: argparse.Namespace) -> "HadoopJobConfig":
        d = vars(args).copy()

        # Convert string to Enum for map_compress_codec if needed
        if "map_compress_codec" in d and isinstance(d["map_compress_codec"], str):
            try:
                d["map_compress_codec"] = CompressionCodec[d["map_compress_codec"]]
            except KeyError:
                raise ValueError(
                    f"Invalid map_compress_codec '{d['map_compress_codec']}', "
                    f"must be one of {[e.name for e in CompressionCodec]}"
                )

        return cls.model_validate(d)

    def to_argparse(self) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(description="A Python wrapper for Hadoop job configuration")

        # Create argument groups
        groups = {}
        for name, field in self.model_fields.items():
            group_name = field.title if field.title else GENERAL_GROUP
            if group_name not in groups:
                groups[group_name] = parser.add_argument_group(group_name)

        for name, field in self.model_fields.items():
            meta = field.json_schema_extra or {}
            group_name = field.title if field.title else GENERAL_GROUP
            group = groups[group_name]

            short_flag = f"-{field.alias}"
            flags = [f"--{name}"]
            if short_flag:
                flags.insert(0, short_flag)

            help_text = field.description if field.description else ""
            default_value = getattr(self, name)
            arg_type = field.annotation

            # Handle Enums (case-insensitive)
            if isinstance(default_value, Enum):
                choices = [e.name.upper() for e in type(default_value)]
                group.add_argument(
                    *flags,
                    type=str.upper,  # parse upper-case user input
                    choices=choices,
                    default=default_value.name.upper(),
                    help=f"{help_text} (options: {', '.join(choices)}, default: {default_value.name.upper()})"
                )

            # Handle human-readable sizes
            elif meta.get("human_readable", False):
                group.add_argument(
                    *flags,
                    type=parse_size,
                    default=default_value,
                    help=f"{help_text} (accepts 256MB, 1G, etc., default: {default_value} bytes)"
                )

            # Handle booleans
            elif arg_type is bool:
                if default_value is True:
                    group.add_argument(*flags, action="store_false", help=f"{help_text} (default: True)")
                else:
                    group.add_argument(*flags, action="store_true", help=f"{help_text} (default: False)")

            # Handle other types (int, float, str, etc.)
            else:
                group.add_argument(
                    *flags,
                    type=arg_type,
                    default=default_value,
                    help=f"{help_text} (default: {default_value})"
                )

        return parser

    def get_hadoop_job_args(self) -> List[str]:
        """
        :return: A ready-to-use list of strings, which can be fed directly to subprocess.Popen, subprocess.run, etc.
        Creating a subprocess using this return value will run the distributed Hadoop job using the parameters
        configured in this class.
        """
        job_str = str(self)
        cleaned_cmd = re.sub(r"\s+", " ", job_str.strip())
        return shlex.split(cleaned_cmd)

    def __str__(self) -> str:
        return f"""
hadoop jar /opt/hadoop-3.4.1/share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar
  -D mapreduce.job.maps={self.number_of_mappers}
  -D mapreduce.job.reduces={self.number_of_reducers}
  -D mapreduce.map.memory.mb={self.map_memory_mb}
  -D mapreduce.reduce.memory.mb={self.reduce_memory_mb}
  -D yarn.app.mapreduce.am.resource.mb={self.application_manager_memory_mb}
  -D mapreduce.map.cpu.vcores={self.map_vcores}
  -D mapreduce.reduce.cpu.vcores={self.reduce_vcores}
  -D yarn.app.mapreduce.am.resource.cpu-vcores={self.application_manager_vcores}
  -D mapreduce.task.io.sort.mb={self.sort_buffer_mb}
  -D mapreduce.task.io.sort.factor={self.io_sort_factor}
  -D mapreduce.map.output.compress={str(self.should_compress).lower()}
  -D mapreduce.map.output.compress.codec={self.map_compress_codec.value}
  -D mapreduce.input.fileinputformat.split.maxsize={self.max_split_size}
  -D mapreduce.input.fileinputformat.split.minsize={self.min_split_size}
  -D mapreduce.reduce.shuffle.parallelcopies={self.shuffle_copies}
  -D mapreduce.job.jvm.numtasks={self.jvm_numtasks}
  -D mapreduce.job.reduce.slowstart.completedmaps={self.slowstart_completed_maps}

  -input hdfs://namenode-1:9000{self.input_path}
  -output hdfs://namenode-1:9000{self.output_path}
  -mapper {self.mapper_path}
  -reducer {self.reducer_path}
  -file {self.mapper_path}
  -file {self.reducer_path}
"""
