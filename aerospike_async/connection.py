import socket
import time

class Connection:
    def __init__(self, address: str, port: int):
        socket_address = (address, port)
        self.socket = socket.create_connection(socket_address)
        self.last_used = time.time_ns()
