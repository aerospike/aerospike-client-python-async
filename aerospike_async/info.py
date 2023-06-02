from typing import Optional

from .connection import Connection
from exceptions import AerospikeException

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

        self.offset = 0
        return buf

    @staticmethod
    async def request(conn: Connection, commands: list[str]):
        info = Info(commands)
        buf = await info.send_command(conn)
        return info.parse_multi_response(buf)

    @staticmethod
    async def new(conn: Connection, command: str):
        # TODO: use buffer
        info = Info([command])
        await info.send_command(conn)
        return info

    def parse_multi_response(self, buf: bytes) -> dict[str, str]:
        # If command is invalid, the command and value for it will not be returned from the server (in the buffer)
        responses = {}
        res = str(buf, encoding="utf-8")
        res = res.split()
        for i in range(0, len(res), 2):
            responses[res[i]] = res[i + 1]
        return responses

    def skip_to_value(self):
        while self.offset < self.length:
            b = self.buffer[self.offset]

            if b == '\t':
                self.offset += 1
                break

            if b == '\n':
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
        if expected != self.buffer[self.offset]:
            raise AerospikeException(f"Expected {expected} Received: {self.buffer[self.offset]}")
        self.offset += 1

    def parse_string(self, stop1: str, stop2: Optional[str] = None, stop3: Optional[str] = None) -> str:
        begin = self.offset
        while self.offset < self.length:
            b = self.buffer[self.offset]
            if b == stop1:
                break
            if (stop2 != None and b == stop2) or (stop3 != None and b == stop3):
                break
            self.offset += 1
        # If string doesn't exist, an empty string is returned
        return str(self.buffer[begin:self.offset], encoding='utf-8')
