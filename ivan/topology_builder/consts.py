from pathlib import Path
import yaml
from ivan.topology_builder.devices_position_config import DevicesPositionConfig


GNS3_URL = "http://192.168.140.110:3080/v2"
PROJECT_ID = "8b1df7a1-d0ea-4bd9-a5d7-f0c6bb61b40b"
IMAGES_TO_REFRESH = ["resourcemanager", "namenode", "historyserver", "datanode"]


with open(Path("topology_builder") / "config.yaml") as f:
    config = yaml.safe_load(f)
    USERNAME = config["username"]
    PASSWORD = config["password"]


hadoop_nodes_replicas = {
    "resourcemanager": 1,
    "namenode": 1,
    "historyserver": 1,
    "datanode": 3,
}

constant_nodes_replicas = {
    "Ethernet switch": 1,
    "NAT": 1,
}

hadoop_devices_position_config = DevicesPositionConfig(
    x_start=-200,
    x_stop=200,
    y_top=-100,
    y_bottom=100,
    number_of_rows=2,
)

constant_devices_position_config = DevicesPositionConfig(
    x_start=-0,
    x_stop=150,
    y_top=-15,
    y_bottom=15,
    number_of_rows=1,
)
