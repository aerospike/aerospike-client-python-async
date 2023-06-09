from dataclasses import dataclass

import time
import asyncio

@dataclass
class Connection:
    s_reader: asyncio.StreamReader
    s_writer: asyncio.StreamWriter
    conn_timeout_secs: float
    last_used_time_ns: int = time.time_ns()

    @staticmethod
    async def new(address: str, port: int, conn_timeout_secs: float):
        # TODO: assign pool
        open_conn = asyncio.open_connection(address, port)
        s_reader, s_writer = await asyncio.wait_for(open_conn, timeout=conn_timeout_secs)
        conn = Connection(s_reader, s_writer, conn_timeout_secs)
        return conn

    async def read(self, num_bytes: int) -> bytes:
        read_cour = self.s_reader.readexactly(num_bytes)
        res = await asyncio.wait_for(read_cour, timeout=self.conn_timeout_secs)
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
