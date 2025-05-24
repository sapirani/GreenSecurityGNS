import sys
from typing import List
from pathlib import Path

from ivan.nodes_configuration_code.find_volumes import find_recent_volume_dirs, find_scanner_volume_dirs

TEMPLATE_PATH = r"./program_parameters_template.py"
PROGRAM_PARAMETERS_FILE_NAME = "program_parameters.py"


def change_parameters_in_scanner_volumes(scanner_volumes_directories: List[str]):
    with open(TEMPLATE_PATH, "r") as template_file:
        template_content = template_file.read()
        for scanner_volume in scanner_volumes_directories:
            path_to_change = Path(scanner_volume) / Path(PROGRAM_PARAMETERS_FILE_NAME)
            print(f"Changing parameters in scanner dir: {scanner_volume}\n")
            with open(path_to_change, "w") as program_parameters_container_file:
                program_parameters_container_file.write(template_content)


def main(number_of_containers: int):
    volumes_directories = find_recent_volume_dirs(number_of_containers)
    scanner_volumes_directories = find_scanner_volume_dirs(volumes_directories)

    change_parameters_in_scanner_volumes(scanner_volumes_directories)


if __name__ == "__main__":
    args = sys.argv
    if len(args) < 2:
        raise Exception("Usage: update_volumes.py <number of containers>")

    n_containers = int(args[1])
    main(n_containers)
