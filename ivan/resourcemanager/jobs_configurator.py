import math
import shutil
from enum import Enum
from itertools import product
from pathlib import Path
from typing import List, Iterator, Dict, Any, Union, Iterable, Sequence, Set
from pydantic import BaseModel, model_validator, PrivateAttr
from hadoop_job_config import CompressionCodec, HadoopJobConfig, HDFS_NAMENODE


class ExperimentMode(str, Enum):
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"


class AutomaticExperimentsConfig(BaseModel):
    """
    This class is used to define a grid search of 'HadoopJobConfig's.
    It enables configuring multiple combinations for each field to establish multiple jos configurations easily.
    If a field's value stays None, the default value for the field will be selected.
    """

    model_config = {
        "extra": "forbid"  # Reject unexpected fields
    }

    # Meta parameters
    mode: ExperimentMode = ExperimentMode.SEQUENTIAL
    sleep_between_launches: int = 5

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
    _user_configured_fields: Set[str] = PrivateAttr()

    @staticmethod
    def _is_iterable(val: Any) -> bool:
        # Only list, set, tuple, etc. should be considered as iterables in our case
        return isinstance(val, Iterable) and not isinstance(val, (str, dict, bytes))

    @staticmethod
    def _normalize_output_path(output_paths: List[str], parameters_grid) -> List[str]:
        """
        :return: The same output paths as received if the list length is aligned with the total number of experiments
        that are supposed to run.
        If a single element list has arrived (representing a directory to insert outputs into),
        expand it so each element will look like:
        [<original element>/output_1, <original element>/output_2, <original element>/output_3, ...]

        :raises: ValueError in the cases where the received list length is not 1 or total number of experiments
        """
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

    def _should_use_default(self, job_config_field_name: str) -> bool:
        """
        :return: True if the user did not insert a value to the field (or chose a None value)
        """
        return job_config_field_name not in self._user_configured_fields or getattr(self, job_config_field_name) is None

    def _normalize_fields(self):
        """
        This function is used for dynamically normalizing the fields of this class according to the user's input
        (for easy processing later on).
        I.e., the goal is that every field will be eventually converted to an iterable.
        In addition, this function copies the aliases of fields from HadoopJobConfig fields.

        1. if the user did not insert a value to the field (or chose a None value):
            a list containing one item which is the default value for that field will be chosen.
        2. if the user inserted a single value which is not a list (e.g., 3):
             a list containing one item, which is the user-requested item (e.g., 3 had turned into [3]) be chosen.
        3. if the user inserted a sequence of items (e.g., a range, list, tuple, etc.):
            the value will be this sequence as-is.
        """
        # preserve user configured fields before manipulating all fields in this class
        self._user_configured_fields = self.__pydantic_fields_set__.copy()

        for job_config_field_name, field in HadoopJobConfig.model_fields.items():
            # copy field's metadata from HadoopJobConfig (e.g., alias, description, etc...)
            self.model_fields[job_config_field_name] = field

            # convert each field into a sequence
            if self._should_use_default(job_config_field_name):  # case 1
                setattr(self, job_config_field_name, [field.default])
            else:  # user inserted values to this field
                field_value = getattr(self, job_config_field_name)
                if not self._is_iterable(field_value):  # case 2
                    # convert to a list containing one item only
                    setattr(self, job_config_field_name, [field_value])

    def _generate_experiments_configs(self):
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

    @model_validator(mode="after")
    def _run_all_validators(self):
        self._normalize_fields()
        self._generate_experiments_configs()
        return self

    def get_config_parameters_grid(self) -> Dict[str, List[Any]]:
        """
        :return: a dictionary consisting the exact keys as 'HadoopJobConfig'.
        If called after normalizing fields (this is the initial intention) the value of each item is will be a
        normalized field (a sequence of items), with a default value / user configured values.
        Otherwise, the raw fields (that might be None or single values) will be returned.
        """
        params = {}
        for name, field in HadoopJobConfig.model_fields.items():
            params[name] = getattr(self, name)

        return params

    def all_experiments_configurations(self) -> Iterator[HadoopJobConfig]:
        return self._all_experiments_configs

    def _core_fields_configured_by_user(self) -> Set[str]:
        """
        This class was chosen to implement this functionality since the HadoopJobConfig is usually configured
        externally (via this class or argparse for example).
        So, either way, if we want HadoopJobConfig to be able to identify which fields were configured by the user
        we should not instantiate it without sending all defaults (right now it cannot distinguish if the user
        selected a value that equals to the default or got the default from automatic normalizing).
        I.e., this class should be aware of the user-configured parameters.
        """
        return self._user_configured_fields.intersection(HadoopJobConfig.model_fields.keys())

    def user_selected_fields(self, experiment_config: HadoopJobConfig) -> Dict[str, Any]:
        """
        Each dictionary represents the fields that were modified by the user.
        I.e., each dictionary contains the shortcut names of the modified fields and their selected values by the user.
        """
        return {
            field_name: getattr(experiment_config, field_name)
            for field_name in self._core_fields_configured_by_user()
        }

    def remove_outputs(self):
        for path in self.output_path:
            shutil.rmtree(Path(HDFS_NAMENODE) / Path(path), ignore_errors=True)

    def __str__(self):
        return "\n\n".join(
            f"************************************ Experiment {i + 1} ************************************\n"
            f"{experiment_config}\n"
            f"{experiment_config.format_user_selection(self.user_selected_fields(experiment_config))}"
            for i, experiment_config in enumerate(self._all_experiments_configs)
        )

    def __len__(self) -> int:
        return len(self._all_experiments_configs)
