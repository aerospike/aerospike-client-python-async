from dataclasses import dataclass
from typing import Union
from enum import BinDataType

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
    # TODO: better default?
    local_udf_path: str = None

class Operation:
    pass

class Expression:
    pass

class Condition:
    pass

@dataclass
class BinValueEquals(Condition):
    bin_name: str
    value: Union[str, int]

@dataclass
class BinValueBetween(Condition):
    bin_name: str
    min_val: int
    max_val: int

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

    # TODO
    def operate(self, key: Key, ops: list[Operation]):
        pass

    def touch(self, key: Key):
        pass

    # TODO:
    def batch(self):
        pass

    # TODO
    def truncate(self, namespace: str, set_name: str):
        pass

    # TODO: should be for devops?
    def create_index(
        self,
        namespace: str,
        set_name: str,
        bin_name: str,
        bin_datatype: type,
        index_name: str
    ):
        pass

    def index_remove(self):
        pass

    # Query

    def find(self, condition: Condition):
        pass

    def set_config(self):
        pass
