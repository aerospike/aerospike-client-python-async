from dataclasses import dataclass, field
from typing import Union, Optional, Any, Callable
from functools import partial

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

@dataclass
class ListAppend(Operation):
    bin_name: str
    # TODO: fix type
    value: Any

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

class BatchOpResult:
    record: Optional[Record] = None
    exception: Optional[Exception] = None

class QueryResults:
    def __aiter__(self):
        return self

    async def __anext__(self) -> Record:
        return None

@dataclass
class UDFCall:
    module_name: str
    function_name: str
    arguments: list[Any] = field(default_factory=list)

class RecordInterface:
    async def record_exists(self, user_key: UserKey) -> bool:
        pass

    async def get_record(self, user_key: UserKey, bin_names: Optional[list[str]] = None) -> Record:
        pass

    async def get_records(self, user_keys: list[UserKey], bin_names: Optional[list[str]] = None) -> list[BatchOpResult]:
        pass

    async def put_record(self, user_key: UserKey, bins: Bins):
        pass

    async def put_records(self, user_keys: list[UserKey], bins: Bins) -> list[BatchOpResult]:
        pass

    async def delete_record(self, user_key: UserKey, bin_names: Optional[list[str]] = None):
        pass

    async def delete_records(self, user_keys: list[UserKey], bin_names: Optional[list[str]] = None) -> list[BatchOpResult]:
        pass

    async def operate_on_record(self, user_key: UserKey, ops: list[Operation]):
        pass

    async def operate_on_records(self, user_keys: list[UserKey], ops: list[Operation]) -> list[BatchOpResult]:
        pass

    async def touch_record(self, user_key: UserKey):
        pass

    async def touch_records(self, user_key: list[UserKey]) -> list[BatchOpResult]:
        pass

    async def batch_perform_on_records(self, batch_ops: list[partial]) -> list[BatchOpResult]:
        pass

    # UDFs

    async def apply_udf_to_record(self, user_key: UserKey, record_udf_function: UDFCall):
        pass

    # TODO
    async def truncate(self):
        pass

    # Query

    async def find_records(self,
                    bin_name: Optional[str] = None,
                    bin_value_equals: Optional[Union[str, int]] = None,
                    bin_value_min: Optional[int] = None,
                    bin_value_max: Optional[int] = None,
                    ) -> QueryResults:
        pass

    async def find_records_and_apply_record_udf(
                                        self,
                                        record_udf_function: UDFCall,
                                        bin_name: Optional[str] = None,
                                        bin_value_equals: Optional[Union[str, int]] = None,
                                        bin_value_min: Optional[int] = None,
                                        bin_value_max: Optional[int] = None,
                                        ) -> Any:
        pass

    async def find_and_aggregate_records(self,
                                   stream_udf_function: UDFCall,
                                   bin_name: Optional[str] = None,
                                   bin_value_equals: Optional[Union[str, int]] = None,
                                   bin_value_min: Optional[int] = None,
                                   bin_value_max: Optional[int] = None,
                                   ) -> Any:
        pass

@dataclass
class Set(RecordInterface):
    set_name: str

    async def create_index(
        self,
        bin_name: str,
        bin_datatype: type,
        index_name: str
    ):
        pass

    async def index_remove(self):
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

    async def __aenter__(self):
        return self

    async def __aexit__(self, exception_type, exception_value, traceback):
        await self.close()

    def __getitem__(self, namespace) -> Namespace:
        return self.__getattr__(namespace)

    def __getattr__(self, namespace) -> Namespace:
        if type(namespace) != str:
            raise TypeError(f"Namespace {namespace} given. Namespace must be a string!")
        if namespace not in self._namespaces:
            self._namespaces[namespace] = Namespace(namespace)
        return self._namespaces[namespace]

    async def close(self):
        pass

    async def download_udf(self, udf_name: str):
        pass
