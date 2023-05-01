import time

from .connection import Connection
from .policies.info import InfoPolicy

PROTOCOL_VERSION = 2
AS_INFO = 1

class Info:
    buffer: memoryview

    def __init__(self, commands: list[str]):
        request = ""
        for command in commands:
            request += command
            request += "\n"

        # TODO: use buffer pool (preferably without a library)
        print(len(request.encode("utf-8")))
        print(request)
        size = len(request.encode("utf-8"))
        self.buffer = memoryview(bytearray(8 + size))
        # TODO: 
        self.buffer[:2] = bytes([PROTOCOL_VERSION, AS_INFO])
        self.buffer[2:8] = size.to_bytes(length=6, byteorder='big')
        self.buffer[8:] = bytearray(request, encoding="utf-8")
        print(self.buffer)

    async def send_command(self, policy: InfoPolicy, conn: Connection):
        conn.set_timeout(policy.timeout)
        await conn.write(self.buffer)
        self.buffer[:8].tobytes()
        await conn.read()
        print(self.buffer[:8])
        size = int.from_bytes(self.buffer[2:8], byteorder='big')
        print("Results size:", size)
        await conn.read(self.buffer[:size])
        print("Results:", self.buffer[:size])

    @staticmethod
    async def request(policy: InfoPolicy, conn: Connection, commands: list[str]):
        info = Info(commands)
        await info.send_command(policy, conn)
        return info.parse_multi_response()

    def parse_multi_response(self) -> dict[str, str]:
        # If command is invalid, the command and value for it will not be returned from the server (in the buffer)
        responses = {}
        res = str(self.buffer, encoding="utf-8")
        res = res.split()
        for i in range(0, len(res), 2):
            responses[res[i]] = res[i + 1]
        return responses
