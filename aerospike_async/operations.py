from dataclasses import dataclass
from typing import Any

class Operation:
    pass

@dataclass
class ListAppend(Operation):
    bin_name: str
    # TODO: fix type
    value: Any

@dataclass
class ListGetByIndex(Operation):
    bin_name: str
    index: int
