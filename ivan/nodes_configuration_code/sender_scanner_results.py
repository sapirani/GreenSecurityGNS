import os
import subprocess
import sys
import zipfile
from typing import List

RESULTS_DIR_PREFIX = "results_"
OUTPUT_ZIP_NAME = "./all_results.zip"

MAIN_CONTAINERS = ["resourcemanager-1", "namenode-1", "historyserver-1"]

SCANNER_DIRECTORY = "Scanner"
VOLUME_INITIAL_DIRECTORY = r""
VOLUME_MAIN_DIRECTORY = VOLUME_INITIAL_DIRECTORY + r"/green_security_measurements"
VOLUME_TYPE = "volume"
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


def find_results_dirs(volume_dirs: List[str], number_of_results_dirs: int) -> List[str]:
    results_dirs = []
    for scanner_vol_dir in find_scanner_volume_dirs(volume_dirs):
        for root, dirs, _ in os.walk(scanner_vol_dir):
            for d in dirs:
                if d.startswith(RESULTS_DIR_PREFIX):
                    results_dirs.append(os.path.join(root, d))
                    if len(results_dirs) == number_of_results_dirs:
                        return results_dirs
    return results_dirs


def extract_container_names(results_paths: List[str]) -> List[str]:
    return [
        os.path.basename(path)[len(RESULTS_DIR_PREFIX):]
        for path in results_paths
        if os.path.basename(path).startswith(RESULTS_DIR_PREFIX)
    ]


def zip_directories(directories: List[str], output_path: str) -> None:
    with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for directory in directories:
            for root, _, files in os.walk(directory):
                for file in files:
                    full_path = os.path.join(root, file)
                    arcname = os.path.relpath(full_path, start=os.path.dirname(directory))
                    zipf.write(full_path, arcname=arcname)
    print(f"Zipped output saved to: {output_path}")


def print_given_containers(results_dirs: List[str], num_of_containers: int) -> None:
    available_datanodes = [f"datanode-{idx + 1}" for idx in range(num_of_containers - len(MAIN_CONTAINERS))]
    expected_containers = MAIN_CONTAINERS + available_datanodes

    found_containers = extract_container_names(results_dirs)
    missing = [c for c in expected_containers if c not in found_containers]
    if missing:
        print(f"Missing results for containers: {missing}")

    else:
        print(f"Found all expected containers: \n{found_containers}")


def main(number_of_containers: int):
    volume_dirs = find_recent_volume_dirs(number_of_containers)
    results_dirs = find_results_dirs(volume_dirs, number_of_containers)

    print("RESULT DIRS:", results_dirs)
    print("\n\n")
    if len(results_dirs) != number_of_containers:
        print("Could not find the required number of result directories across all paths.")

    print_given_containers(results_dirs, number_of_containers)

    print(f"Zipping results from: {results_dirs}")
    zip_directories(results_dirs, OUTPUT_ZIP_NAME)


if __name__ == "__main__":
    args = sys.argv
    if len(args) < 2:
        raise Exception("Usage: sender_scanner_results.py <number of containers>")

    n_containers = int(args[1])
    main(n_containers)
