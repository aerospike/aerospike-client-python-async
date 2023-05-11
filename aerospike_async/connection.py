import time
import socket
import asyncio

class Connection:
    last_time_used: int # TODO
    timeout: float
    s_reader: asyncio.StreamReader
    s_writer: asyncio.StreamWriter

    def __init__(self, s_reader, s_writer):
        self.s_reader = s_reader
        self.s_writer = s_writer

    @staticmethod
    async def new(policy, address: str, port: int):
        s_reader, s_writer = await asyncio.open_connection(address, port)
        conn = Connection(s_reader, s_writer)
        socket_address = (address, port)
        # TODO: set timeout for connect
        return conn

    def set_timeout(self, timeout: float):
        self.timeout = timeout

    async def read(self, num_bytes: int) -> bytes:
        # TODO: handle exceptions here
        # TODO: take care of setting timeout
        # self.stream.socket.settimeout(self.timeout)
        res = await self.s_reader.readexactly(num_bytes)
        self.last_time_used = time.time_ns()
        return res

    async def write(self, buf: bytes):
        # TODO: take care of setting timeout
        # self.stream.socket.settimeout(self.timeout)
        self.s_writer.write(buf)
        await self.s_writer.drain()
        self.last_time_used = time.time_ns()
