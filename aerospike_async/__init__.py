from dataclasses import dataclass
from typing import Union, Optional, Any, Callable

from .host import Host

MapKey = Union[str, bytes, bytearray, int, float]
BinName = str
BinValue = Union[bool, bytes, bytearray, float, int, str, list["BinValue"], dict[MapKey, "BinValue"]]
Bins = dict[BinName, BinValue]

@dataclass
class ClientConfig:
    config_file_path: Optional[str] = None
    local_udf_dir: Optional[str] = "~/.python_client_udfs"

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

@dataclass
class RecordKey:
    namespace: str
    set_name: str
    user_key: UserKey

@dataclass
class Record:
    key: RecordKey
    metadata: Metadata
    bins: Bins

@dataclass
class BatchOperation:
    function: Callable
    args: dict[str, Any]

class QueryResults:
    def __iter__(self):
        return self

    def __next__(self) -> Record:
        return None

class RecordInterface:
    def exists(self, user_key: UserKey) -> bool:
        pass

    def get_record(self, user_key: UserKey, bin_names: Optional[list[str]] = None) -> Record:
        pass

    def put_record(self, user_key: UserKey, bins: Bins):
        pass

    def delete_record(self, user_key: UserKey, bin_names: Optional[list[str]] = None):
        pass

    def operate_on_record(self, user_key: UserKey, ops: list[Operation]):
        pass

    def touch_record(self, user_key: UserKey):
        pass

    def batch_perform_on_record(self, ops: list[BatchOperation]):
        pass

    # TODO
    def truncate(self):
        pass

    # Query

    def find(self, condition: Condition) -> QueryResults:
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

class Namespace(RecordInterface):
    def __init__(self, namespace: str):
        self._namespace = namespace
        self._sets = {}

    def __getitem__(self, set_name: str) -> Set:
        return self.__getattr__(set_name)

    def __getattr__(self, set_name: str) -> Set:
        if type(set_name) != str:
            raise TypeError("Set name {set_name} given. Set name must be a string!")

        if set_name not in self._sets:
            self._sets[set_name] = Set(set_name)
        return self._sets[set_name]

class AsyncClient:
    def __init__(self, hosts: list[Host], config: Optional[ClientConfig] = None):
        self._namespaces = {}
        self.config = config

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.close()

    def __getitem__(self, namespace) -> Namespace:
        return self.__getattr__(namespace)

    def __getattr__(self, namespace) -> Namespace:
        if type(namespace) != str:
            raise TypeError(f"Namespace {namespace} given. Namespace must be a string!")
        if namespace not in self._namespaces:
            self._namespaces[namespace] = Namespace(namespace)
        return self._namespaces[namespace]

    def close(self):
        pass

    def download_udf(self, udf_name: str):
        pass
