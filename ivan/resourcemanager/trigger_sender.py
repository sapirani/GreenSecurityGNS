import os
import subprocess
from enum import Enum
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, PrivateAttr
from hadoop_job_config import HadoopJobConfig


class TriggerAction(str, Enum):
    START_MEASUREMENT = "start_measurement"
    STOP_MEASUREMENT = "stop_measurement"
    STOP_PROGRAM = "stop_program"


class TriggerSender(BaseModel):
    session_id_prefix: str = Field("", description="Control the session id sent to the scanner")
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
    def _build_cmd(self, action: TriggerAction, *, session_id: Optional[str] = None):
        cmd = [
            self._python_path,
            "-m",
            self._module,
            action,
        ]

        full_session_id = self.session_id_prefix
        if session_id:
            full_session_id += f":{session_id}" if self.session_id_prefix else session_id

        if full_session_id:
            cmd.extend(["--session_id", f"{full_session_id}"])

        cmd.extend(["--receivers_addresses", ",".join(self.get_receivers_addresses())])

        return cmd

    @staticmethod
    def generate_session_id(*, generate_session_from: Dict[str, Any]) -> str:
        return "-".join(
            [
                f"{HadoopJobConfig.model_fields[field_name].alias}_{field_value}"
                for field_name, field_value in generate_session_from.items()
            ]
        )

    # TODO: unify this functionality with scanner_trigger from the other repo
    def _run_trigger(self, action: TriggerAction, *, session_id: Optional[str] = None):
        subprocess.run(
            self._build_cmd(action, session_id=session_id),
            env={**os.environ, **self._env},
            check=True
        )

    # TODO: unify this functionality with scanner_trigger from the other repo
    def start_measurement(self, *, session_id: Optional[str] = None):
        self._run_trigger(TriggerAction.START_MEASUREMENT, session_id=session_id)

    # TODO: unify this functionality with scanner_trigger from the other repo
    def stop_measurement(self):
        self._run_trigger(TriggerAction.STOP_MEASUREMENT)

    # TODO: unify this functionality with scanner_trigger from the other repo
    def stop_program(self):
        self._run_trigger(TriggerAction.STOP_PROGRAM)


