import os
import sys
import subprocess
from typing import List
from pathlib import Path


TEMPLATE_PATH = r"./program_parameters_template.py"
PROGRAM_PARAMETERS_FILE_NAME = "program_parameters.py"

SCANNER_DIRECTORY = "Scanner"
VOLUME_INITIAL_DIRECTORY = r"/gns3volumes"
VOLUME_MAIN_DIRECTORY = VOLUME_INITIAL_DIRECTORY + r"/green_security_measurements"
VOLUME_TYPE = "bind"
COMMAND_FORMAT = f"""'{{{{range.Mounts}}}}{{{{if and (eq .Type "{VOLUME_TYPE}") (eq .Destination "{VOLUME_MAIN_DIRECTORY}")}}}}{{{{.Source}}}}{{{{"\\n"}}}}{{{{end}}}}{{{{end}}}}'"""


def run_command_in_dir(command: str) -> List[str]:
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    output = result.stdout.strip().split("\n")
    if result.returncode != 0 or not output:
        raise RuntimeError(f"Failed to run command: {result.stderr}")
    return output


def find_recent_volume_dirs(count: int) -> List[str]:
    command = f"""docker ps -a -q | head -n {count} | xargs docker inspect --format {COMMAND_FORMAT}"""
    volume_dirs = run_command_in_dir(command)
    return volume_dirs

def find_scanner_volume_dirs(volume_dirs: List[str]) -> List[str]:
    return [os.path.join(vol_dir, SCANNER_DIRECTORY) for vol_dir in volume_dirs if vol_dir != ""]


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
