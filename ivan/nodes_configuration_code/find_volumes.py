import os
import subprocess
from typing import List

SCANNER_DIRECTORY = "Scanner"
VOLUME_MAIN_DIRECTORY = r"/green_security_measurements"
COMMAND_FORMAT = f"""'{{{{range.Mounts}}}}{{{{if and (eq .Type "volume") (eq .Destination "{VOLUME_MAIN_DIRECTORY}")}}}}{{{{.Source}}}}{{{{"\\n"}}}}{{{{end}}}}{{{{end}}}}'"""


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
    return [os.path.join(vol_dir, SCANNER_DIRECTORY) for vol_dir in volume_dirs]


