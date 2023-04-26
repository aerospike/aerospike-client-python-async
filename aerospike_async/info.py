from .connection import Connection
import time

PROTOCOL_VERSION = 2
AS_INFO = 1

class Info:
    buffer: bytes

    def __init__(self, commands: list[str]):
        request = ""
        for command in commands:
            request += command
            request += "\n"
        # TODO: use buffer pool (preferably without a library)
        self.buffer = bytes(request, encoding="utf-8")

        proto_header = bytes([PROTOCOL_VERSION, AS_INFO])
        buffer_size = len(self.buffer).to_bytes(length=6, byteorder='big')
        proto_header += buffer_size
        self.buffer = proto_header + self.buffer

    async def send_command(self, conn: Connection):
        await conn.write(self.buffer)
        proto_header = await conn.read(8)
        size = int.from_bytes(proto_header[2:], byteorder='big')
        self.buffer = await conn.read(size)

    @staticmethod
    async def request(conn: Connection, commands: list[str]):
        info = Info(commands)
        await info.send_command(conn)
        return info.parse_multi_response()

    def parse_multi_response(self) -> dict[str, str]:
        # If command is invalid, the command and value for it will not be returned from the server (in the buffer)
        responses = {}
        res = str(self.buffer, encoding="utf-8")
        res = res.split()
        for i in range(0, len(res), 2):
            responses[res[i]] = res[i + 1]
        return responses
