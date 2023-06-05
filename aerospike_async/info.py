from typing import Optional

from .connection import Connection
from enum import IntEnum
from .exceptions import AerospikeException

PROTOCOL_VERSION = 2
AS_INFO = 1

class Info:
    buffer: bytearray

    def __init__(self, commands: list[str]):
        request = ""
        for command in commands:
            request += command
            request += "\n"

        size = len(request.encode("utf-8"))
        self.buffer = bytearray(8 + size)
        # Aerospike protocol header and info message
        self.buffer[:2] = bytes([PROTOCOL_VERSION, AS_INFO])
        self.buffer[2:8] = size.to_bytes(length=6, byteorder='big')
        self.buffer[8:] = bytearray(request, encoding="utf-8")

    async def send_command(self, conn: Connection) -> bytes:
        await conn.write(self.buffer)
        buf = await conn.read(8)
        self.length = int.from_bytes(buf[2:8], byteorder='big')
        buf = await conn.read(self.length)
        self.buffer = bytearray(buf)

        self.offset = 0
        return buf

    @staticmethod
    async def request(conn: Connection, commands: list[str]):
        info = Info(commands)
        buf = await info.send_command(conn)
        return info.parse_multi_response(buf)

    @staticmethod
    async def new(conn: Connection, commands: list[str]):
        # TODO: use buffer
        info = Info(commands)

        await info.send_command(conn)
        return info

    def parse_multi_response(self, buf: bytes) -> dict[str, str]:
        offset = 0
        begin = 0
        responses = {}
        while offset < self.length:
            b = self.buffer[offset]
            if b == ord('\t'):
                name = str(self.buffer[begin:offset], encoding='utf-8')
                offset += 1
                self.check_error()
                begin = offset

                # Parse field value
                while offset < self.length:
                    if self.buffer[offset] == ord('\n'):
                        break
                    offset += 1

                if offset > begin:
                    # Non-empty value for this field was returned
                    value = str(self.buffer[begin:offset], encoding='utf-8')
                    responses[name] = value
                else:
                    # No value was returned for this field
                    responses[name] = None
            elif b == ord('\n'):
                if offset > begin:
                    name = str(self.buffer[begin:offset], encoding='utf-8')
                    responses[name] = None
                offset += 1
                begin = offset
            else:
                offset += 1

        # TODO: If only one field was returned with no value?
        if offset > begin:
            name = str(self.buffer[begin:offset], encoding='utf-8')
            responses[name] = None
        return responses

    def skip_to_value(self):
        while self.offset < self.length:
            b = self.buffer[self.offset]
            if b == ord('\t'):
                self.offset += 1
                break

            if b == ord('\n'):
                break

            self.offset += 1

    def parse_int(self) -> int:
        begin = self.offset
        end = self.offset
        while self.offset < self.length:
            b = self.buffer[self.offset]
            if b < 48 or b > 57:
                # Encountered end of integer
                end = self.offset
                break
            self.offset += 1
        # If integer doesn't exist, 0 is returned
        val = int.from_bytes(self.buffer[begin:end], byteorder='big')
        return val

    def expect(self, expected: str):
        if ord(expected) != self.buffer[self.offset]:
            raise AerospikeException(f"Expected {expected} Received: {chr(self.buffer[self.offset])}")
        self.offset += 1

    def parse_string(self, stop1: str, stop2: Optional[str] = None, stop3: Optional[str] = None) -> str:
        begin = self.offset
        while self.offset < self.length:
            b = self.buffer[self.offset]
            if b == ord(stop1):
                break
            if (stop2 != None and b == ord(stop2)) or (stop3 != None and b == ord(stop3)):
                break
            self.offset += 1
        # If string doesn't exist, an empty string is returned
        return str(self.buffer[begin:self.offset], encoding='utf-8')

    def parse_name(self, name: str):
        begin = self.offset
        while self.offset < self.length:
            if self.buffer[self.offset] == ord('\t'):
                s = str(self.buffer[begin:self.offset], encoding='utf-8')
                if name == s:
                    self.offset += 1
                    # Check for error message after request name
                    # <request>\t<error>\n
                    self.check_error()
                    return
                break
            self.offset += 1

        raise AerospikeException(f"Failed to find {name}")

	# Check if the info command returned an error.
	# If so, include the error code and string in the exception.
    def check_error(self):
        if self.offset + 4 >= self.length:
            return

        if str(self.buffer[self.offset:self.offset + 5], encoding='utf-8') != "ERROR":
            return

        self.offset += 5
        self.skip_delimiter(':')

        begin = self.offset
        code = self.parse_int()

        if code == 0:
            code = ResultCode.SERVER_ERROR

        if self.offset > begin:
            self.skip_delimiter(':')
        elif self.buffer[self.offset] == ord(':'):
            # TODO: ERROR:: is possible?
            self.offset += 1

        message = self.parse_string('\n')

        raise AerospikeException(code, message)

    # Find next delimiter and skip over it
    def skip_delimiter(self, stop: str):
        while self.offset < self.length:
            b = self.buffer[self.offset]
            self.offset += 1
            if b == ord(stop):
                break

class ResultCode(IntEnum):
    SERVER_ERROR = 1
