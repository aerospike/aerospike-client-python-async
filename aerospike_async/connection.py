import asyncio
import socket
import time

class Connection:
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    last_time_used: int # TODO
    timeout: float

    def __init__(self):
        pass

    @staticmethod
    async def new(policy, address: str, port: int):
        conn = Connection()
        conn.reader, conn.writer = await asyncio.open_connection(address, port)
        return conn

    def set_timeout(self, timeout: float):
        self.timeout = timeout

    # TODO: use buffer pool
    async def read(self, n: int):
        res = await asyncio.wait_for(self.reader.readexactly(n), self.timeout)
        self.last_time_used = time.time_ns()
        return res

    # TODO: use buffer pool
    async def write(self, buf: bytes):
        self.writer.write(buf)
        await asyncio.wait_for(self.writer.drain(), self.timeout)
        self.last_time_used = time.time_ns()
