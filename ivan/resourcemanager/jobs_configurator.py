from enum import Enum
from itertools import product
from typing import List, Iterator, Dict, Any, Union
from pydantic import BaseModel, model_validator, PrivateAttr
from ivan.resourcemanager.hadoop_job_config import CompressionCodec, HadoopJobConfig


class ExperimentMode(str, Enum):
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"


class AutomaticExperimentsConfig(BaseModel):
    """
    This class is basically a wrapper for 'HadoopJobConfig'.
    It enables configuring multiple combinations for each field to establish multiple jos configurations easily.
    """

    # Meta parameters
    mode: ExperimentMode = ExperimentMode.SEQUENTIAL
    sleep_between_launches: int = 5

    # Task Definition
    input_path: Union[str, List[str], None] = None
    output_path: Union[str, List[str], None] = None
    mapper_path: Union[str, List[str], None] = None
    reducer_path: Union[str, List[str], None] = None

    # Parallelism & Scheduling
    number_of_mappers: Union[int, List[int], None] = None
    number_of_reducers: Union[int, List[int], None] = None
    map_vcores: Union[int, List[int], None] = None
    reduce_vcores: Union[int, List[int], None] = None
    application_manager_vcores: Union[int, List[int], None] = None
    shuffle_copies: Union[int, List[int], None] = None
    jvm_numtasks: Union[int, List[int], None] = None
    slowstart_completed_maps: Union[float, List[float], None] = None

    # Memory
    map_memory_mb: Union[int, List[int], None] = None
    reduce_memory_mb: Union[int, List[int], None] = None
    application_manager_memory_mb: Union[int, List[int], None] = None
    sort_buffer_mb: Union[int, List[int], None] = None
    min_split_size: Union[int, List[int], None] = None
    max_split_size: Union[int, List[int], None] = None

    # Shuffle & Compression
    io_sort_factor: Union[int, List[int], None] = None
    should_compress: Union[bool, List[bool], None] = None
    map_compress_codec: Union[CompressionCodec, List[CompressionCodec], None] = None

    # Private fields
    _all_experiments_configs: List[HadoopJobConfig] = PrivateAttr()

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

    @staticmethod
    def _should_use_default(job_config_field_name: str, user_inputs: Dict[str, Any]) -> bool:
        """
        :return: True if the user did not insert a value to the field (or chose a None value)
        """
        return job_config_field_name not in user_inputs or user_inputs[job_config_field_name] is None

    @model_validator(mode="before")
    @classmethod
    def normalize_inputs(cls, user_inputs: Dict[str, Any]) -> Dict[str, Any]:
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
            if cls._should_use_default(job_config_field_name, user_inputs):     # case 1
                user_inputs[job_config_field_name] = [field.default]
            else:   # user inserted values to this field
                if not isinstance(user_inputs[job_config_field_name], list):    # case 2
                    # convert to a list containing one item only
                    user_inputs[job_config_field_name] = [user_inputs[job_config_field_name]]

        return user_inputs

    @model_validator(mode="after")
    def generate_experiments_configs(self) -> "AutomaticExperimentsConfig":
        """
        This function validates that all configuration combinations provided by the user are valid.
        If they are, it saves all configurations as a list of'HadoopJobConfig' object that could be fetched later on.
        """
        self._all_experiments_configs = []

        parameters_grid = self.get_config_parameters_grid()
        keys = list(parameters_grid.keys())
        for job_configuration_values in product(*parameters_grid.values()):
            self._all_experiments_configs.append(
                HadoopJobConfig.model_validate(dict(zip(keys, job_configuration_values)))
            )

        return self

    def all_experiments_configurations(self) -> Iterator[HadoopJobConfig]:
        return self._all_experiments_configs

    def __len__(self) -> int:
        return len(self._all_experiments_configs)
