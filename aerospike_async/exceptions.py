from .command import ResultCode, Partition
from . import Policy
from dataclasses import dataclass
from typing import Optional
from .cluster import Node

class AerospikeException(RuntimeError):
    def __init__(self, message: Optional[str] = None, result_code: int = ResultCode.CLIENT_ERROR):
        if message == None:
            super().__init__()
        else:
            super().__init__(message)
        self._result_code = result_code

    def keep_connection(self) -> bool:
        return ResultCode.keep_connection(self._result_code)

    def get_result_code(self) -> int:
        return self._result_code
    
    def set_node(self, node: Node):
        self.node = node

    def set_policy(self, policy: Policy):
        self.policy = policy

    # Get number of attempts before failing
    def set_iteration(self, iteration: int):
        self.iteration = iteration

class InvalidNodeException(AerospikeException):
    def __init__(self, message: Optional[str] = None, cluster_size: Optional[int] = None, partition: Optional[Partition] = None):
        if cluster_size == None and partition == None and message != None:
            super().__init__(message, ResultCode.INVALID_NODE_ERROR)
        elif cluster_size != None and partition != None:
            if cluster_size == 0:
                super().__init__("Cluster is empty")
            else:
                super().__init__(f"Node not found for partition {partition}")

class InvalidQueryException(AerospikeException):
    pass

class InvalidNamespaceException(AerospikeException):
    def __init__(self, ns: str, map_size: int):
        if map_size == 0:
            message = "Partition map empty"
        else:
            message = f"Namespace not found in partition map: {ns}"
        super().__init__(message, ResultCode.INVALID_NAMESPACE)

class ParseException(AerospikeException):
    def __init__(self, message: str):
        super().__init__(message, ResultCode.PARSE_ERROR)

class TimeoutException(AerospikeException):
    def __init__(self, policy: Policy, client: bool):
        super().__init__(result_code=ResultCode.TIMEOUT)
        self.connect_timeout = policy.connect_timeout
        self.socket_timeout = policy.socket_timeout
        self.timeout = policy.total_timeout
        self.client = client

class ConnectionException(AerospikeException):
    def __init__(self, message: str):
        super().__init__(message, ResultCode.SERVER_NOT_AVAILABLE)
