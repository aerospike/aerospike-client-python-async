from dataclasses import dataclass

import time
import asyncio
from typing import Optional
from .cluster import Pool

@dataclass
class Connection:
    s_reader: asyncio.StreamReader
    s_writer: asyncio.StreamWriter
    conn_timeout_secs: float
    pool: Optional[Pool]
    last_used_time_ns: int = time.time_ns()

    @staticmethod
    async def new(address: str, port: int, conn_timeout_secs: float, pool: Optional[Pool]):
        open_conn = asyncio.open_connection(address, port)
        s_reader, s_writer = await asyncio.wait_for(open_conn, timeout=conn_timeout_secs)
        conn = Connection(s_reader, s_writer, conn_timeout_secs, pool)
        return conn

    async def read(self, num_bytes: int) -> bytes:
        read_cour = self.s_reader.readexactly(num_bytes)
        try:
            res = await asyncio.wait_for(read_cour, timeout=self.conn_timeout_secs)
        except TimeoutError as e:
            raise ReadTimeoutException()
        self.last_used_time_ns = time.time_ns()
        return res

    async def write(self, buf: bytes):
        self.s_writer.write(buf)
        write_cour = self.s_writer.drain()
        await asyncio.wait_for(write_cour, timeout=self.conn_timeout_secs)
        self.last_used_time_ns = time.time_ns()

    async def close(self):
        self.s_writer.close()
        await self.s_writer.wait_closed()

    # TODO: different from java implementation
    def is_closed(self):
        return self.s_writer.is_closing()

    def set_timeout(self, timeout: int):
        self.conn_timeout_secs = timeout

class ReadTimeoutException(RuntimeError):
    # TODO: missing a bunch of arguments
    def __init__(self):
        super().__init__("timeout")
