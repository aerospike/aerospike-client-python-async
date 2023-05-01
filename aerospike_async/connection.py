import time
import socket
from tornado.iostream import IOStream
import asyncio

class Connection:
    last_time_used: int # TODO
    timeout: float
    stream: IOStream

    def __init__(self, stream: IOStream):
        self.stream = stream

    @staticmethod
    async def new(policy, address: str, port: int):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        conn = Connection(IOStream(sock))
        socket_address = (address, port)
        # TODO: set timeout for connect
        await conn.stream.connect(socket_address)
        return conn

    def set_timeout(self, timeout: float):
        self.timeout = timeout

    async def read(self, buf: bytearray):
        # TODO: handle exceptions here
        self.stream.socket.settimeout(self.timeout)
        await self.stream.read_into(buf)
        self.last_time_used = time.time_ns()

    async def write(self, buf: bytes):
        self.stream.socket.settimeout(self.timeout)
        await self.stream.write(buf)
        self.last_time_used = time.time_ns()
