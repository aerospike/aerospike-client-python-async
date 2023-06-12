class AerospikeException(Exception):
    pass

class InvalidNodeException(AerospikeException):
    pass

class InvalidQueryException(AerospikeException):
    pass

class InvalidNamespaceException(AerospikeException):
    pass

class InvalidUserKeyTypeException(AerospikeException):
    pass
