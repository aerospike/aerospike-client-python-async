from __future__ import annotations

from dataclasses import dataclass
import ipaddress
import socket

@dataclass
class Host:
    name: str
    port: int
    tls_name: str

    async def resolve(self) -> list[Host]:
        addrs = socket.getaddrinfo(self.name, self.port, family=socket.AF_INET, proto=socket.IPPROTO_TCP)
        res: list[Host] = []
        for addr in addrs:
            _, _, _, _, (ip, port) = addr
            res.append(Host(ip, port, self.tls_name))

        return res

    def is_loopback(self) -> bool:
        try:
            return ipaddress.ip_address(self.name).is_loopback
        except:
            return False

    def is_ip(self) -> bool:
        try:
            ipaddress.ip_address(self.name)
            return True
        except:
            return False
