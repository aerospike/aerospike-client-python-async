import time
import socket
import asyncio

class Connection:
    lut_ns: int  # last used time
    timeout_secs: float
    s_reader: asyncio.StreamReader
    s_writer: asyncio.StreamWriter

    def __init__(self, s_reader, s_writer, timeout_secs: float):
        self.s_reader = s_reader
        self.s_writer = s_writer
        self.timeout_secs = timeout_secs

    @staticmethod
    async def new(address: str, port: int, timeout_secs: float):
        s_reader, s_writer = await asyncio.open_connection(address, port)
        conn = Connection(s_reader, s_writer, timeout_secs)
        return conn

    async def read(self, num_bytes: int) -> bytes:
        read_call = self.s_reader.readexactly(num_bytes)
        res = await asyncio.wait_for(read_call, timeout=self.timeout_secs)
        self.lut_ns = time.time_ns()
        return res

    async def write(self, buf: bytes):
        self.s_writer.write(buf)
        write_call = self.s_writer.drain()
        await asyncio.wait_for(write_call, timeout=self.timeout_secs)
        self.lut_ns = time.time_ns()

    async def close(self):
        self.s_writer.close()
        await self.s_writer.wait_closed()
