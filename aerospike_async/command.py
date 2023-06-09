from typing import Union
from enum import IntEnum, auto
from abc import ABC, abstractmethod
import time
from dataclasses import dataclass
from .cluster import Cluster, Partitions, Node
from .exceptions import AerospikeException, InvalidNodeException
from . import Key, Bins, BinValue

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
        partition_map = cluster.partition_map
        partitions = partition_map.get(key.namespace)
        if partitions == None:
            # TODO: be specific about exception
            raise AerospikeException(f"Invalid namespace {key.namespace}")

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
        # node_array = cluster.get_nodes()
        raise InvalidNodeException()

class Operation:
    class Type(IntEnum):
        WRITE = auto()

class Command:
    MSG_TOTAL_HEADER_SIZE = 30
    MSG_REMAINING_HEADER_SIZE = 22
    FIELD_HEADER_SIZE = 5
    OPERATION_HEADER_SIZE = 8

    INFO2_WRITE = 1

    def __init__(self, socket_timeout: int, total_timeout: int, max_retries: int):
        self.data_offset = 0
        self.max_retries = max_retries
        self.total_timeout = total_timeout

        if total_timeout > 0:
            self.socket_timeout = (socket_timeout < total_timeout)
            # TODO: leave off from here

    def set_write(self, operation: Operation, key: Key, bins: Bins):
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
        # TODO: don't need to clear result code and set unused buffer to 0
        # already done by initializing bytearray
        self.data_buffer[14:18] = int.to_bytes(generation, length=4, byteorder='big')
        # TODO: check policy expiration
        self.data_buffer[18:22] = int.to_bytes(0, length=4, byteorder='big')
        self.data_buffer[22:26] = int.to_bytes(self.ser, length=4, byteorder='big')

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

class AsyncCommand(ABC):
    cluster: Cluster

    # TODO: policy
    def __init__(self, cluster: Cluster):
        self.cluster = cluster
        self.deadline = 0

    # NOTE: execute() from this class's instance instead of event loop
    async def execute(self):
        # TODO: check total timeout
        await self.execute_command()

    async def execute_command(self):
        # TODO: declare vars
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
                conn = await node.get_connection()

                try:
                    # Set command buffer
                    self.write_buffer()

    @abstractmethod
    def get_node(self):
        return None

    def reset_deadline(self, start_time: int):
        elapsed = time.time_ns() - start_time
        self.deadline += elapsed

    @abstractmethod
    def write_buffer(self):
        return None

class AsyncWrite(AsyncCommand):
    # TODO: listener, write policy
    # TODO: use bins type, not dict
    def __init__(self, cluster: Cluster, key: Key, bins: Bins, operation: Operation.Type):
        super().__init__(cluster)
        self.key = key
        self.partition: Partition = Partition.write(cluster, key)
        self.bins = bins
        self.operation = operation

    def get_node(self):
        return self.partition.get_node_write(self.cluster)

    def write_buffer(self):
        # TODO: write policy
        self.set_write(self.operation, self.key, self.bins)
