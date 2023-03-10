import asyncio
from Crypto.Hash import RIPEMD160
from enum import Enum, IntEnum
from typing import Union
import msgpack
from bitarray import bitarray

SERVER_IP = "127.0.0.1"
SERVER_PORT = 3000

# Wire protocol for Aerospike messages

class FieldType(IntEnum):
    AS_MSG_FIELD_TYPE_NAMESPACE = 0
    AS_MSG_FIELD_TYPE_DIGEST_RIPE = 4

class Field:
    def __init__(self, field_type: FieldType, data: bytes):
        self.field_type = field_type
        self.data = data

class OperationType(IntEnum):
    AS_MSG_OP_READ = 1

class ServerType(IntEnum):
    AS_BYTES_INTEGER = 1
    AS_BYTES_STRING = 3
    AS_BYTES_BLOB = 4
    AS_BYTES_MAP = 19
    AS_BYTES_LIST = 20

class InvalidUserKeyTypeException(Exception):
    def __str__(self):
        return "User key must be either a str, int, bytes, or bytearray"

class ServerException(Exception):
    def __init__(self, code):
        self.code = code

    def __str__(self):
        return f"Server returned error code {self.code}"

async def send_as_message(
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
    # message header
    # TODO: not sure if these are the right defaults
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

        field_type = int(field.field_type).to_bytes(1, byteorder='big')
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

    result_code = response_msg[5]
    if result_code != 0:
        raise ServerException(code=result_code)

    def slice_msg_with_len(start: int, len: int):
        return response_msg[start:(start + len)]

    results = {}

    n_ops = int.from_bytes(response_msg[20:22], byteorder='big')
    curr_op_offset = response_msg[0] # after response message header
    for _ in range(n_ops):
        bin_name_len = slice_msg_with_len(start=curr_op_offset + 7, len=1)
        bin_name_len = int.from_bytes(bin_name_len, byteorder='big')

        bin_name = slice_msg_with_len(start=curr_op_offset + 8, len=bin_name_len)
        bin_name = str(bin_name, encoding='utf-8')

        # Decode bin value

        bin_data_type = slice_msg_with_len(curr_op_offset + 5, 1)
        bin_data_type = int.from_bytes(bin_data_type, byteorder='big')

        op_sz = slice_msg_with_len(curr_op_offset, 4)
        op_sz = int.from_bytes(op_sz, byteorder='big')
        bin_value = slice_msg_with_len(
            start=curr_op_offset + 8 + bin_name_len,
            len=op_sz - (4 + bin_name_len)
        )
        if bin_data_type == ServerType.AS_BYTES_STRING:
            bin_value = str(bin_value, encoding='utf-8')
        elif bin_data_type == ServerType.AS_BYTES_INTEGER:
            bin_value = int.from_bytes(bin_value, byteorder='big')
        elif (
            bin_data_type == ServerType.AS_BYTES_MAP
            or
            bin_data_type == ServerType.AS_BYTES_LIST
        ):
            bin_value = msgpack.unpackb(
                bin_value,
                # Server encodes strings as bytes with a type byte at the beginning
                # so we need to manually decode bytes and check if it is a string
                raw=True,
                # Aerospike maps supports more than just the JSON key types
                strict_map_key=False,
                object_hook=object_hook,
                list_hook=list_hook
            )

        results[bin_name] = bin_value
        curr_op_offset += (4 + op_sz)

    writer.close()
    await writer.wait_closed()

    return results

def unpack_strs_and_bytes(item):
    if type(item) == bytes:
        if item[0] == ServerType.AS_BYTES_STRING:
            item = str(item, encoding='utf-8')
            item = item[1:]
        elif item[0] == ServerType.AS_BYTES_BLOB:
            item = bytearray(item)
            item = item[1:]
            item = bytes(item)
    return item

def object_hook(obj: dict):
    for key, value in obj.copy().items():
        new_key = unpack_strs_and_bytes(key)
        value = unpack_strs_and_bytes(value)
        if new_key != key:
            del obj[key]
        obj[new_key] = value
    return obj

def list_hook(obj: list):
    for idx, item in enumerate(obj):
        obj[idx] = unpack_strs_and_bytes(item)
    return obj

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
        user_key_type = ServerType.AS_BYTES_STRING
    elif type(key.user_key) == int:
        user_key_type = ServerType.AS_BYTES_INTEGER
    elif (
        type(key.user_key) == bytes
        or
        type(key.user_key) == bytearray
    ):
        user_key_type = ServerType.AS_BYTES_BLOB
    else:
        raise InvalidUserKeyTypeException

    user_key_type = user_key_type.to_bytes(1, byteorder='big')
    h.update(user_key_type)

    if type(key.user_key) == str:
        encoded_user_key = key.user_key.encode('utf-8')
    elif type(key.user_key) == int:
        # Integer takes up 8 bytes in Aerospike
        encoded_user_key = key.user_key.to_bytes(8, byteorder='big')
    else:
        # user key is a bytes or bytearray object
        encoded_user_key = key.user_key
    h.update(encoded_user_key)

    digest = h.digest()
    return digest

class AsyncClient:
    async def get(self, key: Key) -> dict:
        digest = calculate_digest(key)
        response = await send_as_message(
            AS_MSG_INFO1_READ = 1,
            AS_MSG_INFO1_GET_ALL = 1,
            fields=[
                # Don't need to pass in the record's set
                # digest already uniquely identifies record in a namespace
                Field(
                    FieldType.AS_MSG_FIELD_TYPE_NAMESPACE,
                    key.namespace.encode('utf-8')
                ),
                Field(
                    FieldType.AS_MSG_FIELD_TYPE_DIGEST_RIPE,
                    digest
                )
            ]
        )
        return response
