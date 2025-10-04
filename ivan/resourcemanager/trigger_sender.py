import os
import subprocess
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field, PrivateAttr


class TriggerAction(str, Enum):
    START_MEASUREMENT = "start_measurement"
    STOP_MEASUREMENT = "stop_measurement"
    STOP_PROGRAM = "stop_program"


class TriggerSender(BaseModel):
    measurement_session_id: Optional[str] = Field(None, description="Control the session id sent to the scanner")
    number_of_datanodes: int = Field(3, gt=0, description="number of hadoop_workers")

    resource_manager_url: str = "resourcemanager-1:65432"
    namenode_url: str = "namenode-1:65432"
    history_server_url: str = "historyserver-1:65432"

    datanode_prefix: str = Field("datanode", description="Hostname prefix for each DataNode")
    datanode_port: int = Field(65432, description="Port for each DataNode")

    _python_path: str = PrivateAttr("/green_security_measurements/green_security_venv/bin/python")
    _module: str = PrivateAttr("scanner_trigger.trigger_sender")
    _env: dict = PrivateAttr({"PYTHONPATH": "/green_security_measurements/Scanner"})

    @property
    def datanodes_urls(self) -> List[str]:
        return [
            f"{self.datanode_prefix}-{i}:{self.datanode_port}"
            for i in range(1, self.number_of_datanodes + 1)
        ]

    def get_receivers_addresses(self) -> List[str]:
        return [self.resource_manager_url, self.namenode_url, self.history_server_url, *self.datanodes_urls]

    # TODO: remove function when unifying repos
    def _build_cmd(self, action: TriggerAction):
        cmd = [
            self._python_path,
            "-m",
            self._module,
            action,
        ]

        if self.measurement_session_id:
            cmd.extend(["--session_id", self.measurement_session_id])

        cmd.extend(["--receivers_addresses", ",".join(self.get_receivers_addresses())])

        return cmd

    # TODO: unify this functionality with scanner_trigger from the other repo
    def _run_trigger(self, action: TriggerAction):
        subprocess.run(
            self._build_cmd(action),
            env={**os.environ, **self._env},
            check=True
        )

    # TODO: unify this functionality with scanner_trigger from the other repo
    def start_measurement(self):
        self._run_trigger(TriggerAction.START_MEASUREMENT)

    # TODO: unify this functionality with scanner_trigger from the other repo
    def stop_measurement(self):
        self._run_trigger(TriggerAction.STOP_MEASUREMENT)

    # TODO: unify this functionality with scanner_trigger from the other repo
    def stop_program(self):
        self._run_trigger(TriggerAction.STOP_PROGRAM)


