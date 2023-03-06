import asyncio

SERVER_IP = "127.0.0.1"
SERVER_PORT = 3000

# Wire protocol

PROTOCOL_VERSION = 2
# 1 = Aerospike info
MESSAGE_TYPE = 1

async def info(command: str) -> str:
    command += "\n"

    payload = bytes([PROTOCOL_VERSION, MESSAGE_TYPE])
    messageBytesCount = len(command)
    # TCP uses big endian
    payload += messageBytesCount.to_bytes(length=6, byteorder="big")
    payload += bytes(command, encoding="utf-8")

    reader, writer = await asyncio.open_connection(SERVER_IP, SERVER_PORT)

    writer.write(payload)
    await writer.drain()

    data = await reader.readline()
    # Get value after header
    data = str(data[8:], encoding="utf-8")

    writer.close()
    await writer.wait_closed()

    return data
