from .connection import Connection
import time

PROTOCOL_VERSION = 2
AS_INFO = 1

class Info:
    def __init__(self, conn: Connection, commands: list[str]):
        self.buffer = ""
        for command in commands:
            self.buffer += command
            self.buffer += "\n"
        self.buffer = bytes(self.buffer, encoding="utf-8")

        proto_header = bytes([PROTOCOL_VERSION, AS_INFO])
        buffer_size = len(self.buffer).to_bytes(length=6, byteorder='big')
        proto_header += buffer_size
        self.buffer = proto_header + self.buffer

        self.send_command(conn)

    def send_command(self, conn: Connection):
        conn.socket.send(self.buffer)

        proto_header = conn.socket.recv(8)
        size = int.from_bytes(proto_header[2:], byteorder='big')
        self.buffer = conn.socket.recv(size)
        conn.last_used = time.time_ns()

    @staticmethod
    def request(conn: Connection, commands: list[str]):
        info = Info(conn, commands)
        return info.parse_multi_response()

    def parse_multi_response(self) -> dict[str, str]:
        # If command is invalid, the command and value for it will not be returned from the server (in the buffer)
        responses = {}
        self.buffer = str(self.buffer, encoding="utf-8")
        self.buffer = self.buffer.split()
        for i in range(0, len(self.buffer), 2):
            responses[self.buffer[i]] = self.buffer[i + 1]
        return responses
