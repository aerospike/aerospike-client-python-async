from typing import Union
from enum import IntEnum, auto
from abc import ABC, abstractmethod
import time
from dataclasses import dataclass
from .cluster import Cluster, Partitions, Node
from .exceptions import AerospikeException, InvalidNodeException, InvalidNamespaceException, TimeoutException, ConnectionException
from . import Key, Bins, BinValue, WritePolicy, Policy
from .connection import Connection, ReadTimeoutException

# Partition to read from
class Partition:
    partitions: Partitions
    namespace: str
    partition_id: int

    # TODO: missing replica policy, prevNode, linearize
    def __init__(self, partitions: Partitions, key: Key):
        self.partitions = partitions
        self.partition_id = self.get_partition_id(key.digest)
        self.sequence = 0

    @staticmethod
    def get_partition_id(digest: bytes) -> int:
        # TODO: algorithm doesn't exactly match
        return int(digest[:12]) % Node.PARTITIONS

    @staticmethod
    # TODO: add policy
    def write(cluster: Cluster, key: Key):
		# Must copy hashmap reference for copy on write semantics to work.
        partition_map = cluster.partition_map.copy()
        partitions = partition_map.get(key.namespace)
        if partitions == None:
            # TODO: be specific about exception
            raise InvalidNamespaceException(f"Invalid namespace {key.namespace}")

        return Partition(partitions, key)

    def get_node_write(self, cluster: Cluster) -> Node:
        # TODO: Check replica option
        return self.get_sequence_node(cluster)

    def get_sequence_node(self, cluster: Cluster):
        replicas = self.partitions.replicas
        max_ = len(replicas)
        for i in range(max_):
            index = self.sequence % max_
            node = replicas[index][self.partition_id]

            if node != None and node.active:
                return node
            self.sequence += 1
        # TODO: be specific about exception
        node_array = cluster.get_nodes()
        raise InvalidNodeException(cluster_size=len(node_array), partition=self)

class OperationType:
    WRITE = (2, True)

    def __init__(self, protocol_type: int, is_write: bool):
        self.protocol_type = protocol_type
        self.is_write = is_write

class FieldType(IntEnum):
    NAMESPACE = 0
    TABLE = 1
    DIGEST_RIPE = 4

class Buffer:
    @staticmethod
    def long_to_bytes(v: int, buf: bytearray, offset: int):
        buf[offset:offset + 8] = int.to_bytes(v, length=8, byteorder='big')

    @staticmethod
    def int_to_bytes(v: int, buf: bytearray, offset: int):
        buf[offset:offset + 4] = int.to_bytes(v, length=4, byteorder='big')

    @staticmethod
    def short_to_bytes(v: int, buf: bytearray, offset: int):
        buf[offset:offset + 2] = int.to_bytes(v, length=2, byteorder='big')

    @staticmethod
    def string_to_utf8(s: str, buf: bytearray, offset: int) -> int:
        # TODO: not doing it the java way
        encoded = bytes(s, encoding='utf-8')
        buf[offset:offset + len(encoded)] = encoded

class ParticleType(IntEnum):
    INTEGER = 1
    STRING = 3
    BLOB = 4

class Command:
    STATE_READ_HEADER = 2

    MSG_TOTAL_HEADER_SIZE = 30
    MSG_REMAINING_HEADER_SIZE = 22
    FIELD_HEADER_SIZE = 5
    OPERATION_HEADER_SIZE = 8
    CL_MSG_VERSION = 2
    AS_MSG_TYPE = 3

    INFO2_WRITE = 1

    def __init__(self, socket_timeout: int, total_timeout: int, max_retries: int):
        self.data_offset = 0
        self.max_retries = max_retries
        self.total_timeout = total_timeout

        if total_timeout > 0:
            if socket_timeout < total_timeout and socket_timeout > 0:
                self.socket_timeout = socket_timeout
            else:
                self.socket_timeout = total_timeout
            self.server_timeout = self.socket_timeout
        else:
            self.socket_timeout = socket_timeout
            self.server_timeout = 0

    def set_write(self, operation: OperationType, key: Key, bins: Bins):
        self.begin()
        # TODO: pass in policy
        field_count = self.estimate_key_size(key)

        # TODO: check for policy filter exp

        for bin_name, bin_value in bins.items():
            # TODO: does not match java implementation
            self.estimate_operation_size(bin_name, bin_value)

        # TODO: sizeBuffer()
        self.data_buffer = bytearray(30)
        self.write_header_write(self.INFO2_WRITE, field_count, len(bins))
        # TODO: set policy
        self.write_key(key)

        # TODO: check filter expression

        for bin in bins.items():
            self.write_operation(bin, operation)

        self.end()
        # TODO: compress

    def end(self):
        # Write total size of message which is the current offset.
        proto = (self.data_offset - 8) | (self.CL_MSG_VERSION << 56) | (self.AS_MSG_TYPE << 48)
        Buffer.long_to_bytes(proto, self.data_buffer, 0)

    def write_operation(self, bin: tuple[str, BinValue], operation: OperationType):
        bin_name = bin[0]
        bin_value = bin[1]
        name_length = Buffer.string_to_utf8(bin_name, self.data_buffer, self.data_offset + self.OPERATION_HEADER_SIZE)
        value_length = self.write_bin_value(bin_value, self.data_buffer, self.data_offset + self.OPERATION_HEADER_SIZE + name_length)

        Buffer.int_to_bytes(name_length + value_length + 4, self.data_buffer, self.data_offset)
        self.data_offset += 4
        self.data_buffer[self.data_offset] = operation.protocol_type
        self.data_offset += 1
        self.data_buffer[self.data_offset] = self.get_bin_value_type(bin_value)
        self.data_offset += 1
        self.data_buffer[self.data_offset] = 0
        self.data_offset += 1
        self.data_buffer[self.data_offset] = name_length
        self.data_offset += 1
        self.data_offset += name_length + value_length

    @staticmethod
    def get_bin_value_type(bin_value: BinValue) -> int:
        type_to_int = {
            int: ParticleType.INTEGER,
            str: ParticleType.STRING,
            bytearray: ParticleType.BLOB
            # TODO: support other particle types
        }
        return type_to_int[type(bin_value)]

    def write_bin_value(self, bin_value: BinValue, buffer: bytearray, offset: int) -> int:
        if type(bin_value) == str:
            return Buffer.string_to_utf8(bin_value, buffer, offset)
        elif type(bin_value) == int:
            Buffer.long_to_bytes(bin_value, buffer, offset)
            return 8

    def write_key(self, key: Key):
        # Write key into buffer
        if key.namespace != None:
            self.write_str_field(key.namespace, FieldType.NAMESPACE)

        if key.set_name != None:
            self.write_str_field(key.set_name, FieldType.TABLE)

        self.write_bytes_field(key.digest, FieldType.DIGEST_RIPE)

        # TODO: check for policy send key

    def write_bytes_field(self, b: bytes, field_type: int):
        start = self.data_offset + self.FIELD_HEADER_SIZE
        end = start + len(b)
        self.data_buffer[start:end] = b
        self.write_field_header(len(b), field_type)
        self.data_offset += len(b)

    def write_str_field(self, string: str, field_type: int):
        length = Buffer.string_to_utf8(string, self.data_buffer, self.data_offset + self.FIELD_HEADER_SIZE)
        self.write_field_header(length, field_type)
        self.data_offset += length

    def write_field_header(self, size: int, field_type: int):
        Buffer.int_to_bytes(size + 1, self.data_buffer, self.data_offset)
        self.data_offset += 4
        self.data_buffer[self.data_offset] = field_type
        self.data_offset += 1

    def write_header_write(self, write_attr: int, field_count: int, operation_count: int):
        generation = 0
        read_attr = 0
        info_attr = 0

        # TODO: check record exists policy option
        # TODO: check generation policy
        # TODO: check commit level
        # TODO: check durable delete
        # TODO: check xdr

		# Write all header data except total size which must be written last.
        self.data_buffer[8] = self.MSG_REMAINING_HEADER_SIZE # Message header length
        self.data_buffer[9] = read_attr
        self.data_buffer[10] = write_attr
        self.data_buffer[11] = info_attr
        # Unused buffer and result code are already 0 by initializing bytearray
        Buffer.int_to_bytes(generation, self.data_buffer, 14)
        # TODO: check policy expiration
        Buffer.int_to_bytes(0, self.data_buffer, 18)
        Buffer.int_to_bytes(self.server_timeout, self.data_buffer, 22)
        Buffer.short_to_bytes(field_count, self.data_buffer, 26)
        Buffer.short_to_bytes(operation_count, self.data_buffer, 28)
        self.data_offset = self.MSG_TOTAL_HEADER_SIZE

    # Command sizing

    # TODO: check if tuple is right type
    def estimate_operation_size(self, bin_name: str, bin_value: BinValue):
        self.data_offset += len(bin_name.encode("utf-8")) + self.OPERATION_HEADER_SIZE
        self.data_offset += self.estimate_bin_value_size(bin_value)

    @staticmethod
    def estimate_bin_value_size(bin_value: BinValue) -> int:
        if type(bin_value) == str:
            return len(bin_value.encode(encoding='utf-8'))
        elif type(bin_value)== int:
            return 8
        elif type(bin_value) == bytearray or type(bin_value) == bytes:
            return len(bin_value)
        else:
            # TODO
            raise AerospikeException("Unsupported bin value type")

    def estimate_key_size(self, key: Key) -> int:
        field_count = 0

        if key.namespace != None:
            # TODO: actually estimate the size without encoding the string
            self.data_offset += len(key.namespace.encode('utf-8')) + self.FIELD_HEADER_SIZE
            field_count += 1

        if key.set_name != None:
            self.data_offset += len(key.set_name.encode('utf-8')) + self.FIELD_HEADER_SIZE
            field_count += 1

        self.data_offset += len(key.digest) + self.FIELD_HEADER_SIZE
        field_count += 1

        # TODO: check for policy send key
        return field_count

    def begin(self):
        self.data_offset = self.MSG_TOTAL_HEADER_SIZE

class ResultCode(IntEnum):
    SERVER_NOT_AVAILABLE = -8
    INVALID_NODE_ERROR = -3
    PARSE_ERROR = -2
    CLIENT_ERROR = -1
    TIMEOUT = 9
    INVALID_NAMESPACE = 20
    FILTERED_OUT = 27

    # Shold connection be put back into pool
    @staticmethod
    def keep_connection(result_code: int):
        if result_code <= 0:
            # Do not keep connection on client errors.
            return False

        # TODO: check for certain result codes
        return True

class AsyncCommand(Command, ABC):
    cluster: Cluster

    # TODO: policy
    def __init__(self, cluster: Cluster, policy: Policy):
        super().__init__(policy.socket_timeout, policy.total_timeout, policy.max_retries)
        self.cluster = cluster
        self.policy = policy
        self.deadline = 0
        self.command_sent_counter = 0
        self.iteration = 1

    # NOTE: execute() from this class's instance instead of event loop
    async def execute(self):
        # TODO: check total timeout
        await self.execute_command()

    async def execute_command(self):
        # TODO: declare vars
        is_client_timeout = False
		# Execute command until successful, timed out or maximum iterations have been reached.
        while True:
            try:
                node = self.get_node()
            except AerospikeException as ae:
                if self.cluster.is_active():
                    # TODO: set ae policy and iteration and in doubt
                    raise ae
                else:
                    raise AerospikeException("Cluster has been closed")

            try:
                node.validate_error_count()
                # TODO: properly get connection timeout from policy
                conn = await node.get_connection(self, 10)

                try:
                    # Set command buffer
                    self.write_buffer()

                    # Send command
                    await conn.write(self.data_buffer)
                    self.command_sent_counter += 1

                    # Parse results
                    await self.parse_result(conn)

                    # Put connection back in pool.
                    await node.put_connection(conn)

                    # Command has completed successfully. Exit method.
                    return
                except AerospikeException as ae:
                    if ae.keep_connection():
                        # Put connection back in pool
                        await node.put_connection(conn)
                    else:
                        # Close socket to flush out possible garbage. Do not put back in pool
                        await node.close_connection(conn)

                    if ae.get_result_code() == ResultCode.TIMEOUT:
                        # Retry on server timeout
                        exception = TimeoutException(self.policy, client=False)
                        is_client_timeout = False
                        node.incr_error_count()
                    # TODO: check for device overload
                    else:
                        raise ae
                except ReadTimeoutException as rte:
                    # TODO: policy timeout delay
                    await node.close_connection(conn)
                    is_client_timeout = True
                except RuntimeError as re:
					# All runtime exceptions are considered fatal.  Do not retry.
					# Close socket to flush out possible garbage.  Do not put back in pool.
                    await node.close_connection(conn)
                    raise re
                # TODO: socket timeout exception?
                except IOError as ioe:
					# IO errors are considered temporary anomalies.  Retry.
                    await node.close_connection(conn)
                    # TODO: create a ConnectionException instead
                    exception = ioe
                    is_client_timeout = False
            except ReadTimeoutException as rte:
                # Connection already handled
                is_client_timeout = True
            # Check for ConnectionException instead as above
            except IOError as ioe:
                # Socket connection error has occurred. Retry.
                exception = ioe
                is_client_timeout = False
            # TODO: check if node is in backoff state
            except AerospikeException as ae:
                ae.set_node(node)
                ae.set_policy(self.policy)
                ae.set_iteration(self.iteration)
                # TODO: set in doubt
                raise ae

            if self.iteration > self.max_retries:
                break

            if self.total_timeout > 0:
                # Check for total timeout
                # TODO: check for sleep between retries
                remaining = self.deadline - time.time_ns()

                if remaining <= 0:
                    break

                # Convert back to milliseconds for remaining check.
                remaining = remaining * 10**3 // 10**9

                if remaining < self.total_timeout:
                    self.total_timeout = remaining

                    if self.socket_timeout > self.total_timeout:
                        self.socket_timeout = self.total_timeout

                # TODO: check for sleep between retries

                self.iteration += 1

                # TODO: retry batch

        # Retries have been exhausted. Throw last exception.
        if is_client_timeout:
            exception = TimeoutException(self.policy, True)

        exception.set_node(node)
        exception.set_policy(self.policy)
        exception.set_iteration(self.iteration)
        # TODO: set in doubt
        raise exception

    @abstractmethod
    def get_node(self) -> Node:
        pass

    def reset_deadline(self, start_time: int):
        elapsed = time.time_ns() - start_time
        self.deadline += elapsed

    @abstractmethod
    def write_buffer(self):
        pass

    @abstractmethod
    def prepare_retry(self, timeout: bool):
        pass

    @abstractmethod
    async def parse_result(self, conn: Connection):
        # Read header.
        # TODO: use read fully instead of just read?
        header = await conn.read(Command.MSG_TOTAL_HEADER_SIZE)

        result_code = self.data_buffer[13] & 0xFF

        if result_code == 0:
            return

        # TODO: check fail on filtered out in policy

        raise AerospikeException(result_code=result_code)

class WriteCommand(AsyncCommand):
    # TODO: listener, write policy
    def __init__(self, cluster: Cluster, write_policy: WritePolicy, key: Key, bins: Bins, operation: OperationType):
        super().__init__(cluster, write_policy)
        self.write_policy = write_policy
        self.key = key
        self.partition: Partition = Partition.write(cluster, key)
        self.bins = bins
        self.operation = operation

    def get_node(self):
        return self.partition.get_node_write(self.cluster)

    def write_buffer(self):
        # TODO: write policy
        self.set_write(self.operation, self.key, self.bins)
