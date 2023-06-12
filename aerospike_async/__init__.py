from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Union, Optional, Any
from functools import partial
from Crypto.Hash import RIPEMD160

from .operations import Operation
from .command import WriteCommand, OperationType
from .cluster import Cluster, Host
from .exceptions import InvalidUserKeyTypeException

MapKey = Union[str, bytes, bytearray, int, float]
# Recursively define allowed bin values
BinValue = Union[bool, bytes, bytearray, float, int, str, list["BinValue"], dict[MapKey, "BinValue"]]
Bins = dict[str, BinValue]

# Only file-based configs
@dataclass
class ClientConfig:
    config_file_path: Optional[str] = None
    # TODO: move to config file as a setting
    # All downloaded stream udfs will be stored here and used for client-side aggregations
    local_udf_dir: Optional[str] = "~/.python_client_udfs"

@dataclass
class Metadata:
    ttl: int
    gen: int

UserKey = Union[int, str, bytes, bytearray]

class Key:
    namespace: str
    set_name: Optional[str]
    user_key: UserKey
    digest: bytes

    def __init__(self, namespace: str, set_name: Optional[str], user_key: UserKey):
        self.namespace = namespace
        self.set_name = set_name
        self.user_key = user_key
        # Compute digest
        h = RIPEMD160.new()
        # TODO: check if encoding without set is correct
        if set_name != None:
            encoded_set = set_name.encode('utf-8')
            h.update(encoded_set)

        AS_BYTES_INTEGER = 1
        AS_BYTES_STRING = 3
        AS_BYTES_BLOB = 4

        if type(user_key) == str:
            user_key_type = AS_BYTES_STRING
            user_key = user_key.encode('utf-8')
        elif type(user_key) == int:
            user_key_type = AS_BYTES_INTEGER
            # Integer takes up 8 bytes in Aerospike
            user_key = user_key.to_bytes(8, byteorder='big')
        elif (
            type(user_key) == bytes
            or
            type(user_key) == bytearray
        ):
            user_key_type = AS_BYTES_BLOB
        else:
            # TODO: not in java client?
            raise InvalidUserKeyTypeException

        user_key_type = user_key_type.to_bytes(1, byteorder='big')
        h.update(user_key_type)
        h.update(user_key)

        self.digest = h.digest()

# Record data that is returned from the server
@dataclass
class Record:
    key: Key
    metadata: Metadata
    bins: Bins

class BatchOpResult:
    # If this batch operation returns a result, it is stored here
    record: Optional[Record] = None
    # Indicates whether this batch operation caused an exception
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
    # Default is empty list
    arguments: list[Any] = field(default_factory=list)

@dataclass
class Policy:
    # TODO: docstrings
    total_timeout: int
    socket_timeout: int
    max_retries: int

@dataclass
class WritePolicy(Policy):
    pass

@dataclass
class RecordInterface:
    cluster: Cluster
    namespace: str
    set_name: Optional[str]

    async def record_exists(self, user_key: UserKey) -> bool:
        '''
        :param UserKey user_key: The user key of the record.
        :return: a :class:`bool` that is `True` if the record exists in the server, and `False` if it doesn't exist.
        '''
        return False

    async def get_record(self, user_key: UserKey, bin_names: Optional[list[str]] = None) -> Record:
        '''
        Get a record from the server.

        :param UserKey user_key: The user key of the record.
        :param list[str] bin_names: If specified, only get the specified bins in the record. \
            If not specified, get all the bins in the record.
        '''
        return None

    async def get_records(self, user_keys: list[UserKey], bin_names: Optional[list[str]] = None) -> list[BatchOpResult]:
        '''
        Get multiple records from the server.

        :param list[UserKey] user_keys: The user keys of the records.
        :param list[str] bin_names: If specified, only get the specified bins in the records. \
            If not specified, get all the bins of the records.
        :return: A list of :class:`BatchOpResult` objects for each record.
        '''
        return []

    async def put_record(self, policy: WritePolicy, user_key: UserKey, bins: Bins):
        '''
        Put a new record in the server. If the record already exists, update its bins.

        :param UserKey user_key: The user key of the record.
        :param Bins bins: The bins to insert into the record. \
            If the bins already exist, update its values.
        '''
        key = Key(self.namespace, self.set_name, user_key)
        command = WriteCommand(self.cluster, policy, key, bins, OperationType.WRITE)
        await command.execute()

    async def put_records(self, user_keys: list[UserKey], bins: Bins) -> list[BatchOpResult]:
        '''
        Put multiple records in the server with the same bins.

        :param list[UserKey] user_keys: The user keys of the records.
        :param Bins bins: The bins to insert into the record. \
            If the bins already exist, update its values.
        :return: A list of :class:`BatchOpResult` objects for each record.
        '''
        return []

    async def delete_record(self, user_key: UserKey, bin_names: Optional[list[str]] = None):
        '''
        Delete a record or specific record bins from the server.

        :param UserKey user_key: The user key of the record.
        :param list[str] bin_names: If specified, delete these specific bins from the record. \
            If not specified, delete the entire record.
        '''
        pass

    async def delete_records(self, user_keys: list[UserKey], bin_names: Optional[list[str]] = None) -> list[BatchOpResult]:
        '''
        Delete multiple records or specific bins from multiple records from the server.

        :param list[UserKey] user_keys: The user keys of the records.
        :param list[str] bin_names: If specified, delete these specific bins from the records. \
            If not specified, delete all the records.
        :return: A list of :class:`BatchOpResult` objects for each record.
        '''
        return []

    async def operate_on_record(self, user_key: UserKey, ops: list[Operation]) -> Union[Record, None]:
        '''
        Perform a list of operations on a record in the server.

        :param UserKey user_key: The user key of the record.
        :param list[Operation] ops: A list of operations to perform on the record.
        :return: A :class:`Record` object if the list of operations contains at least one read operation. \
            This object will store the results of the read operations. \
            Otherwise, nothing will be returned.
        '''
        pass

    async def operate_on_records(self, user_keys: list[UserKey], ops: list[Operation]) -> list[BatchOpResult]:
        '''
        Perform a list of operations each on multiple records in the server.

        :param list[UserKey] user_keys: The user keys of the records.
        :param list[Operation] ops: A list of operations to perform on the records.
        :return: A list of :class:`BatchOpResult` objects for each record. \
            In each :class:`BatchOpResult` object, a :class:`Record` object will be present if \
            the list of operations contains at least one read operation for that record. \
            This object will store the results of the read operations for that record. \
            Otherwise, the record will be :obj:`None` in the :class:`BatchOpResult` object.
        '''
        return []

    async def touch_record(self, user_key: UserKey):
        '''
        Reset a record's ttl and increment its generation in the server.

        :param UserKey user_key: The user key of the record.
        '''
        pass

    async def touch_records(self, user_key: list[UserKey]) -> list[BatchOpResult]:
        '''
        Reset multiple records' ttls and increment their generations in the server.

        :param list[UserKey] user_keys: The user keys of the records.
        :return: A list of :class:`BatchOpResult` objects for each record.
        '''
        return []

    async def batch_perform_on_records(self, batch_ops: list[partial]) -> list[BatchOpResult]:
        '''
        :param list[partial] batch_ops: A list of single record API calls and its arguments to perform.
        :return: A list of :class:`BatchOpResult` objects for each batch operation.
        '''
        return []

    # UDFs

    async def apply_udf_to_record(self, user_key: UserKey, record_udf_function: UDFCall):
        '''
        Apply a record UDF to a record in the esrver.

        :param UserKey user_key: The user key of the record.
        :param UDFCall record_udf_function: The record UDF and its arguments to apply on the record.
        '''
        pass

    async def truncate(self, nanos: Optional[int] = None):
        '''
        Remove all records in this namespace or set whose last updated time is older than the given time.

        :param nanos int: The time in nanoseconds since the UNIX epoch `(1970-01-01)`. \
            If not specified, remove all the records in the namespace or set.
        '''
        pass

    # Query

    async def find_records(self,
                    bin_name: Optional[str] = None,
                    bin_value_equals: Optional[Union[str, int]] = None,
                    bin_value_min: Optional[int] = None,
                    bin_value_max: Optional[int] = None,
                    record_udf_function: Optional[UDFCall] = None,
                    ) -> QueryResults:
        '''
        Perform a query for all records in the namespace or set.

        :param bin_name str: If specified, only return records that contain this bin. \
            If not specified, return all records in the namespace or set.
        :param bin_value_equals Union[str, int]: If specified, return records in which the specified bin contains this value. \
            This parameter cannot be used with `bin_value_min` or `bin_value_max`.
        :param bin_value_min int: If specified, return records in which the specified bin's value is greater than or equal to this value. \
            This must be used in conjunction with `bin_value_max`.
        :param bin_value_min int: If specified, return records in which the specified bin's value is greater than or equal to this value. \
        :param record_udf_function UDFCall: If specified, apply the record UDF on all records from the query.

        :return: An iterable :class:`QueryResults` to iterate through each :class:`Record` returned from the query.
        :raises: :class:`InvalidQueryException` if `bin_value_equals` is used with either `bin_value_min` or `bin_value_max`.
        '''
        return QueryResults()

    # TODO: maybe combine this with find_records()
    async def find_and_aggregate_records(self,
                                   stream_udf_function: UDFCall,
                                   bin_name: Optional[str] = None,
                                   bin_value_equals: Optional[Union[str, int]] = None,
                                   bin_value_min: Optional[int] = None,
                                   bin_value_max: Optional[int] = None,
                                   ) -> Any:
        '''
        Perform a query for all records in the namespace or set, and then perform an aggregation on the records.

        :param stream_udf_function UDFCall: If specified, apply the stream UDF on all records from the query.
        :param bin_name str: If specified, only return records that contain this bin. \
            If not specified, return all records in the namespace or set.
        :param bin_value_equals Union[str, int]: If specified, return records in which the specified bin contains this value. \
            This parameter cannot be used with `bin_value_min` or `bin_value_max`.
        :param bin_value_min int: If specified, return records in which the specified bin's value is greater than or equal to this value. \
            This must be used in conjunction with `bin_value_max`.
        :param bin_value_min int: If specified, return records in which the specified bin's value is greater than or equal to this value. \

        :return: The results of the stream UDF.
        :raises: :class:`InvalidQueryException` if `bin_value_equals` is used with either `bin_value_min` or `bin_value_max`.
        '''
        pass

@dataclass
class Set(RecordInterface):
    async def create_index(
        self,
        bin_name: str,
        bin_datatype: type,
        index_name: str
    ):
        '''
        Create a secondary index.

        :param bin_name str: The bin to create the set on.
        :param bin_datatype type: The type of bin value to create the set on.
        :param index_name str: The name of the index.
        '''
        pass

    async def index_remove(self, index_name: str):
        '''
        Remove a secondary index.

        :param index_name str: name of the index to remove.
        '''
        pass

class Namespace(RecordInterface):
    def __init__(self, cluster: Cluster, namespace: str):
        super().__init__(cluster, namespace, None)
        self._sets = {}

    def __getitem__(self, set_name: str) -> Set:
        '''
        Use map key syntax to fetch or create a set from a Namespace object.
        Sets are created lazily on the client side, and are not actually created on the server \
        until a server operation is performed.
        '''
        return self.__getattr__(set_name)

    def __getattr__(self, set_name: str) -> Set:
        '''
        Use attribute syntax to fetch or create a set from a Namespace object.
        '''
        if type(set_name) != str:
            raise TypeError("Set name {set_name} given. Set name must be a string!")

        if set_name not in self._sets:
            self._sets[set_name] = Set(self.cluster, self.namespace, set_name)
        return self._sets[set_name]

class AsyncClient:
    cluster: Cluster
    def __init__(self, hosts: list[Host], config: Optional[ClientConfig] = None):
        self._namespaces = {}
        self.config = config

    @staticmethod
    async def new(hosts: list[Host], config: Optional[ClientConfig] = None):
        client = AsyncClient(hosts)
        cluster = await Cluster.new(hosts)
        client.cluster = cluster
        return client

    def __getitem__(self, namespace) -> Namespace:
        '''
        Use map key syntax to fetch a namespace from the client.
        Namespaces are created lazily on the client side. Note that if the namespace doesn't actually exist on the server \
        and a server operation is performed, an exception will be raised.
        The namespace must be created on the server first.
        '''
        return self.__getattr__(namespace)

    def __getattr__(self, namespace) -> Namespace:
        '''
        Use attribute syntax to fetch a namespace from the client.
        '''
        if type(namespace) != str:
            raise TypeError(f"Namespace {namespace} given. Namespace must be a string!")
        if namespace not in self._namespaces:
            self._namespaces[namespace] = Namespace(self.cluster, namespace)
        return self._namespaces[namespace]

    async def close(self):
        '''
        Close connection with the server.

        The client cannot be used again, and a new client must be created to reconnect with the server.
        '''
        pass

    async def download_udf(self, udf_name: str):
        '''
        Download a stream UDF from the server.
        '''
        pass
