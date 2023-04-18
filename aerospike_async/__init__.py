from dataclasses import dataclass
from typing import Union

from .host import Host

class Key:
    pass

MapKey = Union[str, bytes, bytearray, int, float]
BinName = str
BinValue = Union[bool, bytes, bytearray, float, int, str, list["BinValue"], dict[MapKey, "BinValue"]]
Bins = dict[BinName, BinValue]

@dataclass
class ClientConfig:
    file_path: str = None

class Operation:
    pass

class AsyncClient:
    def __init__(self, hosts: list[Host], config: ClientConfig = None):
        pass

    def __enter__(self):
        return self

    def __exit__(self):
        self.close()

    def close(self):
        pass

    def exists(self, key: Key) -> bool:
        pass

    def get(self, key: Key, bin_names: list[str] = None):
        pass

    def put(self, key: Key, bins: Bins):
        pass

    def delete(self, key: Key, bin_names: list[str] = None):
        pass

    def operate(self, key: Key, ops: list[Operation]):
        pass

    def touch(self, key: Key):
        pass

    def set_config(config: ClientConfig):
        pass
