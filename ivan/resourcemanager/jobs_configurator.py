import math
from enum import Enum
from itertools import product
from pathlib import Path
from typing import List, Iterator, Dict, Any, Union, Iterable, Sequence
from pydantic import BaseModel, model_validator, PrivateAttr, Field
from hadoop_job_config import CompressionCodec, HadoopJobConfig


class ExperimentMode(str, Enum):
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"


class AutomaticExperimentsConfig(BaseModel):
    """
    This class is basically a wrapper for 'HadoopJobConfig'.
    It enables configuring multiple combinations for each field to establish multiple jos configurations easily.
    If a field's value stays None, the default value for the field will be selected.
    """

    # Meta parameters
    mode: ExperimentMode = ExperimentMode.SEQUENTIAL
    sleep_between_launches: int = 5
    print_configurations_only: bool = False

    # Task Definition
    input_path: Union[str, Sequence[str], None] = None
    # Receiving an output path as a string will be used as a prefix (a host directory), where the actual outputs
    # will be saved in files named output_1, output_2, etc., inside that host directory.
    output_path: Union[str, Sequence[str], None] = None
    mapper_path: Union[str, Sequence[str], None] = None
    reducer_path: Union[str, Sequence[str], None] = None

    # Parallelism & Scheduling
    number_of_mappers: Union[int, Sequence[int], None] = None
    number_of_reducers: Union[int, Sequence[int], None] = None
    map_vcores: Union[int, Sequence[int], None] = None
    reduce_vcores: Union[int, Sequence[int], None] = None
    application_manager_vcores: Union[int, Sequence[int], None] = None
    shuffle_copies: Union[int, Sequence[int], None] = None
    jvm_numtasks: Union[int, Sequence[int], None] = None
    slowstart_completed_maps: Union[float, Sequence[float], None] = None

    # Memory
    map_memory_mb: Union[int, Sequence[int], None] = None
    reduce_memory_mb: Union[int, Sequence[int], None] = None
    application_manager_memory_mb: Union[int, Sequence[int], None] = None
    sort_buffer_mb: Union[int, Sequence[int], None] = None
    min_split_size: Union[int, Sequence[int], None] = None
    max_split_size: Union[int, Sequence[int], None] = None

    # Shuffle & Compression
    io_sort_factor: Union[int, Sequence[int], None] = None
    should_compress: Union[bool, Sequence[bool], None] = None
    map_compress_codec: Union[CompressionCodec, Sequence[CompressionCodec], None] = None

    # Private fields
    _all_experiments_configs: List[HadoopJobConfig] = PrivateAttr()

    @staticmethod
    def _should_use_default(job_config_field_name: str, user_inputs: Dict[str, Any]) -> bool:
        """
        :return: True if the user did not insert a value to the field (or chose a None value)
        """
        return job_config_field_name not in user_inputs or user_inputs[job_config_field_name] is None

    @staticmethod
    def _is_iterable(val: Any) -> bool:
        # Only list, set, tuple, etc. should be considered as iterables in our case
        return isinstance(val, Iterable) and not isinstance(val, (str, dict, bytes))

    @model_validator(mode="before")  # noqa
    @classmethod
    def _normalize_inputs(cls, user_inputs: Dict[str, Any]) -> Dict[str, Any]:
        """
        This function coerce user inputs to be represented as lists, to ease the work later on
        (e.g., when performing a product of all fields' values to create all requested combinations of Hadoop jobs).

        :return: a dictionary consisting the exact keys as 'HadoopJobConfig'.
        The value of each item is will be as follows:
        1. if the user did not insert a value to the field (or chose a None value):
            a list containing one item which is the default value for that field.
        2. if the user inserted a single value which is not a list (e.g., 3):
             a list containing one item, which is the user-requested item (e.g., 3 had turned into [3])
        3. if the user inserted a list of values:
            the value will be this list as-is.
        """
        for job_config_field_name, field in HadoopJobConfig.model_fields.items():
            if cls._should_use_default(job_config_field_name, user_inputs):  # case 1
                user_inputs[job_config_field_name] = [field.default]
            else:  # user inserted values to this field
                if not cls._is_iterable(user_inputs[job_config_field_name]):  # case 2
                    # convert to a list containing one item only
                    user_inputs[job_config_field_name] = [user_inputs[job_config_field_name]]

        return user_inputs

    @staticmethod
    def _normalize_output_path(output_paths: List[str], parameters_grid) -> List[str]:
        number_of_combinations = math.prod(map(len, parameters_grid.values()))
        if len(output_paths) == number_of_combinations:
            return output_paths
        elif len(output_paths) == 1:
            output_path_prefix = output_paths[0]
            return [f"{Path(output_path_prefix) / Path(f'output_{i + 1}')}" for i in range(number_of_combinations)]

        raise ValueError(
            f"Output path should consists a single value "
            f"or a value for each combination of inputs ({number_of_combinations} in this case)"
        )

    @model_validator(mode="after")
    def _generate_experiments_configs(self) -> "AutomaticExperimentsConfig":
        """
        This function validates that all configuration combinations provided by the user are valid.
        If they are, it saves all configurations as a list of 'HadoopJobConfig' object that could be fetched later on.
        """
        self._all_experiments_configs = []

        parameters_grid = self.get_config_parameters_grid()
        normalized_output_paths = parameters_grid.pop("output_path")
        normalized_output_paths = self._normalize_output_path(normalized_output_paths, parameters_grid)

        keys = list(parameters_grid.keys())
        for job_configuration_values, output_path in zip(product(*parameters_grid.values()), normalized_output_paths):
            self._all_experiments_configs.append(
                HadoopJobConfig.model_validate(dict(zip(keys, job_configuration_values), output_path=output_path))
            )

        return self

    def get_config_parameters_grid(self) -> Dict[str, List[Any]]:
        """
        :return: a dictionary consisting the exact keys as 'HadoopJobConfig'.
        The value of each item is will be as follows:
        1. if the user did not insert a value to the field (or chose a None value):
            a list containing one item which is the default value for that field.
        2. if the user inserted a single value which is not a list (e.g., 3):
             a list containing one item, which is the user-requested item (e.g., 3 had turned into [3])
        3. if the user inserted a list of values:
            the value will be this list as-is.
        """
        params = {}
        for name, field in HadoopJobConfig.model_fields.items():
            params[name] = getattr(self, name)

        return params

    def all_experiments_configurations(self) -> Iterator[HadoopJobConfig]:
        return self._all_experiments_configs

    def __str__(self):
        return "\n\n".join(
            f"********************************** Experiment {i} **********************************\n{experiment_config}"
            for i, experiment_config in enumerate(self._all_experiments_configs)
        )

    def __len__(self) -> int:
        return len(self._all_experiments_configs)
