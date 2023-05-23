from .connection import Connection

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
        size = int.from_bytes(buf[2:8], byteorder='big')
        buf = await conn.read(size)
        return buf

    @staticmethod
    async def request(conn: Connection, commands: list[str]):
        info = Info(commands)
        buf = await info.send_command(conn)
        return info.parse_multi_response(buf)

    def parse_multi_response(self, buf: bytes) -> dict[str, str]:
        # If command is invalid, the command and value for it will not be returned from the server (in the buffer)
        responses = {}
        res = str(buf, encoding="utf-8")
        res = res.split()
        for i in range(0, len(res), 2):
            responses[res[i]] = res[i + 1]
        return responses
