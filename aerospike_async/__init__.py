from dataclasses import dataclass
from typing import Union
from enum import BinDataType

from .host import Host

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

UserKey = Union[int, str, bytes, bytearray]

@dataclass
class Metadata:
    ttl: int
    gen: int

class RecordInterface:
    def get_metadata(self, user_key: UserKey) -> Metadata:
        pass

    def exists(self, user_key: UserKey) -> bool:
        pass

    def get(self, user_key: UserKey, bin_names: list[str] = None) -> Bins:
        pass

    def put(self, user_key: UserKey, bins: Bins):
        pass

    def delete(self, user_key: UserKey, bin_names: list[str] = None):
        pass

    def operate(self, user_key: UserKey, ops: list[Operation]):
        pass

    def touch(self, user_key: UserKey):
        pass

    # TODO:
    def batch(self):
        pass

    # TODO
    def truncate(self):
        pass

    # Query

    def find(self, condition: Condition):
        pass

@dataclass
class Set(RecordInterface):
    set_name: str

    def create_index(
        self,
        bin_name: str,
        bin_datatype: type,
        index_name: str
    ):
        pass

    def index_remove(self):
        pass

@dataclass
class Namespace(RecordInterface):
    def __init__(self, namespace: str):
        self.namespace = namespace
        self.sets = {}

    def __getattr__(self, set_name: str):
        if set_name not in self.sets:
            self.sets[set_name] = Set()

class AsyncClient:
    def __init__(self, hosts: list[Host], config: ClientConfig = None):
        self.namespaces = {}

    def __enter__(self):
        return self

    def __exit__(self):
        self.close()

    def __getattr__(self, namespace: str):
        if namespace not in self.namespaces:
            self.namespaces[namespace] = Namespace(namespace)
        return self.namespaces[namespace]

    def close(self):
        pass

    def set_config(self):
        pass
