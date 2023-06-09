from typing import Union
from enum import IntEnum, auto
from abc import ABC, abstractmethod
import time
from dataclasses import dataclass
from .cluster import Cluster, Partitions, Node
from .exceptions import AerospikeException, InvalidNodeException

# TODO: move to client module because public facing
@dataclass
class Key:
    namespace: str
    set_name: str
    # TODO: digest
    user_key: Union[int, str, bytes, bytearray]
    digest: bytes

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
    FIELD_HEADER_SIZE = 5
    OPERATION_HEADER_SIZE = 8

    def __init__(self):
        self.data_offset = 0

    def set_write(self, operation: Operation, key: Key, bins: dict):
        self.begin()
        # TODO: pass in policy
        field_count = self.estimate_key_size(key)

        # TODO: check for policy filter exp

        for bin in bins.items():
            self.estimate_operation_size(bin)

    # Command sizing

    # TODO: check if tuple is right type
    def estimate_operation_size(self, bin: tuple):
        self.data_offset += len(bin[0].encode("utf-8")) + self.OPERATION_HEADER_SIZE
        self.data_offset += bin[1]

    def estimate_key_size(self, key: Key):
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
    def __init__(self, cluster: Cluster, key: Key, bins: dict, operation: Operation.Type):
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
