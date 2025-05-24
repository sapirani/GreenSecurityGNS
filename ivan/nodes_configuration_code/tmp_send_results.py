import os
import zipfile
from typing import List

from ivan.nodes_configuration_code.find_volumes import find_recent_volume_dirs

# Configuration
NUM_OF_CONTAINERS = 5

SCANNER_DIRECTORY = "Scanner"
RESULTS_DIR_PREFIX = "results_"
OUTPUT_ZIP_NAME = "./all_results.zip"

MAIN_CONTAINERS = ["resourcemanager-1", "namenode-1", "historyserver-1"]
AVAILABLE_DATANODES = [f"datanode{idx + 1}-1" for idx in range(NUM_OF_CONTAINERS - len(MAIN_CONTAINERS))]
ALL_NODES = MAIN_CONTAINERS + AVAILABLE_DATANODES


def find_results_dirs(volume_dirs: List[str]) -> List[str]:
    results_dirs = []
    for vol_dir in volume_dirs:
        search_path = os.path.join(vol_dir, SCANNER_DIRECTORY)
        for root, dirs, _ in os.walk(search_path):
            for d in dirs:
                if d.startswith(RESULTS_DIR_PREFIX):
                    results_dirs.append(os.path.join(root, d))
                    if len(results_dirs) == NUM_OF_CONTAINERS:
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


def main():
    volume_dirs = find_recent_volume_dirs(NUM_OF_CONTAINERS)
    results_dirs = find_results_dirs(volume_dirs)

    print("RESULT DIRS:", results_dirs)
    print("\n\n")
    if len(results_dirs) != NUM_OF_CONTAINERS:
        print("Could not find the required number of result directories across all paths.")

    found_containers = extract_container_names(results_dirs)
    expected_containers = ALL_NODES[:NUM_OF_CONTAINERS]
    missing = [c for c in expected_containers if c not in found_containers]
    if missing:
        print(f"Missing results for containers: {missing}")

    print(f"Zipping results from: {results_dirs}")
    zip_directories(results_dirs, OUTPUT_ZIP_NAME)


if __name__ == "__main__":
    main()
