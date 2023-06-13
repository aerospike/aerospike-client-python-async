from .command import ResultCode
from dataclasses import dataclass

class AerospikeException(Exception):
    result_code: int

    def keep_connection(self) -> bool:
        return ResultCode.keep_connection(self.result_code)

    def get_result_code(self) -> int:
        return self.result_code

class InvalidNodeException(AerospikeException):
    pass

class InvalidQueryException(AerospikeException):
    pass

class InvalidNamespaceException(AerospikeException):
    pass

class InvalidUserKeyTypeException(AerospikeException):
    pass

@dataclass
class Timeout(AerospikeException):
    client: bool
