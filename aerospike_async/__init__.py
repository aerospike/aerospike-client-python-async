import asyncio
from Crypto.Hash import RIPEMD160
from enum import Enum, IntEnum
from typing import Union

from bitarray import bitarray

SERVER_IP = "127.0.0.1"
SERVER_PORT = 3000

# Wire protocol for Aerospike messages

class FieldType(Enum):
    AS_MSG_FIELD_TYPE_NAMESPACE = 0
    AS_MSG_FIELD_TYPE_SET = 1
    AS_MSG_FIELD_TYPE_DIGEST_RIPE = 4

class Field:
    def __init__(self, field_type: FieldType, data: bytes):
        self.field_type = field_type
        self.data = data

# TODO: outdated
class OperationType(Enum):
    AS_MSG_OP_READ = 1
    AS_MSG_OP_WRITE = 2
    AS_MSG_OP_INCR = 5
    AS_MSG_OP_APPEND = 9
    AS_MSG_OP_PREPEND = 10
    AS_MSG_OP_TOUCH = 11

# TODO: outdated
class BinType(IntEnum):
    AS_BYTES_UNDEF = 0
    AS_BYTES_INTEGER = 1
    AS_BYTES_DOUBLE = 2
    AS_BYTES_STRING = 3
    AS_BYTES_BLOB = 4
    AS_BYTES_JAVA = 7
    AS_BYTES_CSHARP = 8
    AS_BYTES_PYTHON = 9
    AS_BYTES_RUBY = 10
    AS_BYTES_PHP = 11
    AS_BYTES_ERLANG = 12
    AS_BYTES_BOOL = 17
    AS_BYTES_HLL = 18
    AS_BYTES_MAP = 19
    AS_BYTES_LIST = 20
    AS_BYTES_GEOJSON = 23
    AS_BYTES_TYPE_MAX = 24

class Operation:
    def __init__(self, op: OperationType, bin_data_type: BinType, bin_name: str, bin_data: bytes):
        self.op = op
        self.bin_data_type = bin_data_type
        self.bin_name = bin_name
        self.bin_data = bin_data

async def send_message(
    # info1
    AS_MSG_INFO1_COMPRESS_RESPONSE = 0,
    AS_MSG_INFO1_CONSISTENCY_LEVEL_ALL = 0,
    AS_MSG_INFO1_GET_NO_BINS = 0,
    AS_MSG_INFO1_XDR = 0,
    AS_MSG_INFO1_BATCH = 0,
    AS_MSG_INFO1_SHORT_QUERY = 0,
    AS_MSG_INFO1_GET_ALL = 0,
    AS_MSG_INFO1_READ = 0,
    # info2
    AS_MSG_INFO2_RESPOND_ALL_OPS = 0,
    AS_MSG_INFO2_CREATE_ONLY = 0,
    AS_MSG_INFO2_DURABLE_DELETE = 0,
    AS_MSG_INFO2_GENERATION_GT = 0,
    AS_MSG_INFO2_GENERATION = 0,
    AS_MSG_INFO2_DELETE = 0,
    AS_MSG_INFO2_WRITE = 0,
    # info3
    AS_MSG_INFO3_SC_READ_RELAX = 0,
    AS_MSG_INFO3_SC_READ_TYPE = 0,
    AS_MSG_INFO3_REPLACE_ONLY = 0,
    AS_MSG_INFO3_CREATE_OR_REPLACE = 0,
    AS_MSG_INFO3_UPDATE_ONLY = 0,
    AS_MSG_INFO3_PARTITION_DONE = 0,
    AS_MSG_INFO3_COMMIT_LEVEL_MASTER = 0,
    AS_MSG_INFO3_LAST = 0,
    # header
    # TODO: fix defaults
    generation = 0,
    record_ttl = 0,
    transaction_ttl = 0,

    fields: list[Field] = [],
):
    MESSAGE_HEADER_SIZE = 22
    message_header = bytearray([MESSAGE_HEADER_SIZE])

    info1_bits = bitarray(
        [
            AS_MSG_INFO1_COMPRESS_RESPONSE,
            AS_MSG_INFO1_CONSISTENCY_LEVEL_ALL,
            AS_MSG_INFO1_GET_NO_BINS,
            AS_MSG_INFO1_XDR,
            AS_MSG_INFO1_BATCH,
            AS_MSG_INFO1_SHORT_QUERY,
            AS_MSG_INFO1_GET_ALL,
            AS_MSG_INFO1_READ,
        ]
    )
    info1_bytes = info1_bits.tobytes()
    message_header += info1_bytes

    info2_bits = bitarray(
        [
            AS_MSG_INFO2_RESPOND_ALL_OPS,
            0,
            AS_MSG_INFO2_CREATE_ONLY,
            AS_MSG_INFO2_DURABLE_DELETE,
            AS_MSG_INFO2_GENERATION_GT,
            AS_MSG_INFO2_GENERATION,
            AS_MSG_INFO2_DELETE,
            AS_MSG_INFO2_WRITE,
        ]
    )
    info2_bytes = info2_bits.tobytes()
    message_header += info2_bytes

    info3_bits = bitarray(
        [
            AS_MSG_INFO3_SC_READ_RELAX,
            AS_MSG_INFO3_SC_READ_TYPE,
            AS_MSG_INFO3_REPLACE_ONLY,
            AS_MSG_INFO3_CREATE_OR_REPLACE,
            AS_MSG_INFO3_UPDATE_ONLY,
            AS_MSG_INFO3_PARTITION_DONE,
            AS_MSG_INFO3_COMMIT_LEVEL_MASTER,
            AS_MSG_INFO3_LAST,
        ]
    )
    info3_bytes = info3_bits.tobytes()
    message_header += info3_bytes

    unused = (0).to_bytes(1, byteorder='big')
    message_header += unused

    result_code = (0).to_bytes(1, byteorder='big')
    message_header += result_code

    generation = generation.to_bytes(4, byteorder='big')
    message_header += generation

    record_ttl = record_ttl.to_bytes(4, byteorder='big')
    message_header += record_ttl

    transaction_ttl = transaction_ttl.to_bytes(4, byteorder='big')
    message_header += transaction_ttl

    n_fields = len(fields)
    n_fields = n_fields.to_bytes(2, byteorder='big')
    message_header += n_fields

    n_ops = (0).to_bytes(2, byteorder='big')
    message_header += n_ops

    fields_bytes = bytearray()

    for field in fields:
        field_bytes = bytearray()

        size = len(field.data) + 1
        size = size.to_bytes(4, byteorder='big')
        field_bytes += size

        field_type = int(field.field_type.value).to_bytes(1, byteorder='big')
        field_bytes += field_type

        field_bytes += field.data

        fields_bytes += field_bytes

    operations_bytes = bytearray()

    message = bytearray()

    message += message_header
    message += fields_bytes
    message += operations_bytes

    PROTOCOL_VERSION = 2
    AEROSPIKE_MESSAGE = 3
    protocol_header = bytearray([PROTOCOL_VERSION, AEROSPIKE_MESSAGE])
    message_size = len(message).to_bytes(6, byteorder='big')
    protocol_header += message_size

    message = protocol_header + message

    reader, writer = await asyncio.open_connection(SERVER_IP, SERVER_PORT)

    writer.write(message)
    await writer.drain()

    response_proto_header = await reader.read(8)
    response_msg_sz = response_proto_header[7]

    response_msg = await reader.read(response_msg_sz)

    n_ops = int.from_bytes(response_msg[20:22], byteorder='big')
    curr_op_offset = response_msg[0] # after response message header

    def slice_with_length(start, len):
        return response_msg[start:(start + len)]

    results = {}

    for _ in range(n_ops):
        bin_name_len = slice_with_length(curr_op_offset + 7, 1)
        bin_name_len = int.from_bytes(bin_name_len, byteorder='big')
        bin_name = slice_with_length(curr_op_offset + 8, bin_name_len)
        bin_name = str(bin_name, encoding='utf-8')

        op_sz = slice_with_length(curr_op_offset, 4)
        op_sz = int.from_bytes(op_sz, byteorder='big')
        bin_value = slice_with_length(curr_op_offset + 8 + bin_name_len, op_sz - (bin_name_len + 4))

        # Decode bin value
        bin_data_type = slice_with_length(curr_op_offset + 5, 1)
        bin_data_type = int.from_bytes(bin_data_type, byteorder='big')
        # TODO: support more bin value types
        if bin_data_type == BinType.AS_BYTES_STRING:
            bin_value = str(bin_value, encoding='utf-8')
        elif bin_data_type == BinType.AS_BYTES_INTEGER:
            bin_value = int.from_bytes(bin_value, byteorder='big')

        results[bin_name] = bin_value

    writer.close()
    await writer.wait_closed()

    return results

UserKey = Union[str, int, bytes, bytearray]

class Key:
    def __init__(self, namespace: str, set: str, user_key: UserKey):
        self.namespace = namespace
        self.set = set
        self.user_key = user_key

def calculate_digest(key: Key) -> bytes:
    h = RIPEMD160.new()

    encoded_set = key.set.encode('utf-8')
    h.update(encoded_set)

    if type(key.user_key) == str:
        encoded_uk_type = BinType.AS_BYTES_STRING
    elif type(key.user_key) == int:
        encoded_uk_type = BinType.AS_BYTES_INTEGER
    elif (
        type(key.user_key) == bytes
        or
        type(key.user_key) == bytearray
    ):
        encoded_uk_type = BinType.AS_BYTES_BLOB
    # TODO: throw error if key is not a valid type?
    encoded_uk_type = encoded_uk_type.value.to_bytes(1, byteorder='big')
    h.update(encoded_uk_type)

    if type(key.user_key) == str:
        encoded_uk = key.user_key.encode('utf-8')
    elif type(key.user_key) == int:
        # Integer takes up 8 bytes in Aerospike
        encoded_uk = key.user_key.to_bytes(8, byteorder='big')
    else:
        encoded_uk = key.user_key
    h.update(encoded_uk)

    digest = h.digest()
    return digest

class AsyncClient:
    async def get(key: Key) -> bytes:
        digest = calculate_digest(key)
        response = await send_message(
            AS_MSG_INFO1_READ = 1,
            AS_MSG_INFO1_GET_ALL = 1,
            fields=[
                # TODO: don't encode here
                Field(
                    FieldType.AS_MSG_FIELD_TYPE_NAMESPACE,
                    key.namespace.encode('utf-8')
                ),
                Field(
                    FieldType.AS_MSG_FIELD_TYPE_SET,
                    key.set.encode('utf-8')
                ),
                Field(
                    FieldType.AS_MSG_FIELD_TYPE_DIGEST_RIPE,
                    digest
                )
            ]
        )
        return response
