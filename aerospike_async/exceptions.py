class AerospikeException(Exception):
    pass

class InvalidNodeException(AerospikeException):
    pass

class InvalidQueryException(Exception):
    pass
