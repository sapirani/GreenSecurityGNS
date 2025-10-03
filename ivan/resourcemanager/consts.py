from enum import Enum
from itertools import product
from typing import Optional, List, Iterator, Dict, Any
from pydantic import BaseModel, model_validator, PrivateAttr
from ivan.resourcemanager.hadoop_job_config import CompressionCodec, HadoopJobConfig


class ExperimentMode(str, Enum):
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"


class AutomaticExperimentsConfig(BaseModel):
    # Meta parameters
    mode: ExperimentMode = ExperimentMode.SEQUENTIAL
    sleep_between_launches: int = 5

    # Task Definition
    input_path: Optional[List[str]] = None
    output_path: Optional[List[str]] = None
    mapper_path: Optional[List[str]] = None
    reducer_path: Optional[List[str]] = None

    # Parallelism & Scheduling
    number_of_mappers: Optional[List[int]] = None
    number_of_reducers: Optional[List[int]] = None
    map_vcores: Optional[List[int]] = None
    reduce_vcores: Optional[List[int]] = None
    application_manager_vcores: Optional[List[int]] = None
    shuffle_copies: Optional[List[int]] = None
    jvm_numtasks: Optional[List[int]] = None
    slowstart_completed_maps: Optional[List[float]] = None

    # Memory
    map_memory_mb: Optional[List[int]] = None
    reduce_memory_mb: Optional[List[int]] = None
    application_manager_memory_mb: Optional[List[int]] = None
    sort_buffer_mb: Optional[List[int]] = None
    min_split_size: Optional[List[int]] = None
    max_split_size: Optional[List[int]] = None

    # Shuffle & Compression
    io_sort_factor: Optional[List[int]] = None
    should_compress: Optional[bool] = None
    map_compress_codec: Optional[CompressionCodec] = None

    _all_experiments_configs: List[HadoopJobConfig] = PrivateAttr()

    def get_parameter_grid(self) -> Dict[str, List[Any]]:
        params = {}
        for name, field in HadoopJobConfig.model_fields.items():
            self_field = getattr(self, name)
            params[name] = [field.default] if self_field is None else self_field

        return params

    @model_validator(mode="after")
    def check_experiments_config_validity(self) -> "AutomaticExperimentsConfig":
        self._all_experiments_configs = []

        parameters_grid = self.get_parameter_grid()
        keys = list(parameters_grid.keys())
        for job_configuration_values in product(*parameters_grid.values()):
            self._all_experiments_configs.append(
                HadoopJobConfig.model_validate(dict(zip(keys, job_configuration_values)))
            )

        return self

    def all_experiments_configurations(self) -> Iterator[HadoopJobConfig]:
        return self._all_experiments_configs
