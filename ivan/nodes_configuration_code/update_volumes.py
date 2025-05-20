import subprocess
from typing import List
from pathlib import Path

TEMPLATE_PATH = r"./program_parameters_template.py"
PROJECT_NUMBER = "e39ba8b9-65fb-4ecd-aaca-be2e4d72c868"
DOCKER_PATH = fr"/opt/gns3/projects/{PROJECT_NUMBER}/project-files/docker"
VOLUME_MAIN_DIRECTORY = r"green_security_measurements"
SCANNER_DIRECTORY = r"Scanner"
PROGRAM_PARAMETERS_FILE_NAME = "program_parameters.py"

NUM_OF_CONTAINERS = 5
COMMAND_FOR_VOLUMES_LIST = f'ls -lat | awk \'$9 != "" {{print $9}}\' | head -n {NUM_OF_CONTAINERS}'
COMMAND_TO_ENTER_DOCKER_DIR = f"cd {DOCKER_PATH}"

def get_volumes_directories() -> List[str]:
    volumes_res = subprocess.run(COMMAND_FOR_VOLUMES_LIST, shell=True, capture_output=True, text=True, cwd=DOCKER_PATH)
    print("Volumes res", volumes_res)
    output_lines = volumes_res.stdout.strip().split("\n")
    print("Output lines", output_lines)
    if isinstance(output_lines, list) and all(isinstance(line, str) for line in output_lines) and len(
            output_lines) == NUM_OF_CONTAINERS:
        print("Valid output:", output_lines)
        return output_lines
    else:
        print("Invalid output")
        raise Exception("Invalid Volumes Names")


def change_parameters_in_volumes(volumes_directories: List[str]):
    with open(TEMPLATE_PATH, "r") as template_file:
        template_content = template_file.read()
        for volume_directory_name in volumes_directories:
            path_to_change = Path(DOCKER_PATH) / Path(volume_directory_name) / Path(VOLUME_MAIN_DIRECTORY) / Path(
                SCANNER_DIRECTORY)

            with open(Path(path_to_change) / Path(PROGRAM_PARAMETERS_FILE_NAME), "w") as program_parameters_container_file:
                program_parameters_container_file.write(template_content)


if __name__ == "__main__":
    volumes_list = get_volumes_directories()
    change_parameters_in_volumes(volumes_list)
