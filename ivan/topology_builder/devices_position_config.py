from dataclasses import dataclass


@dataclass(frozen=True)
class DevicesPositionConfig:
    x_start: int
    x_stop: int
    y_top: int
    y_bottom: int
    number_of_rows: int
