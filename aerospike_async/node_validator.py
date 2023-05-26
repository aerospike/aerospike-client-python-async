from __future__ import annotations

from .host import Host
from .connection import Connection
from .info import Info
import ipaddress
import socket

class NodeValidator:
	name: str
	features: list[str]
	host: Host
	timeout_secs: float

	# host.name MUST be an ip
	def __init__(self, name: str, features: list[str]):
		self.name = name
		self.features = features

	async def new(host: Host, timeout_secs: float) -> NodeValidator:
		is_ip = host.is_ip()
		if not is_ip:
			# TODO(Julian): Introduce an AerospikeException class and use that consistently
			raise Exception("Invalid host passed to node validator")

		try:
			conn = await Connection.new(host.name, host.port, timeout_secs)
			commands = ["node", "features"]

			if not host.is_loopback():
				commands.append(self.service_command())

			info_map = await Info.request(conn, commands)
			if "node" in info_map:
				name = info_map["node"]

			if "features" in info_map:
				features = info_map["features"].split(";")

		finally:
			await conn.close()

		return NodeValidator(name, features)

	def service_command() -> str:
		return 'service-clear-std'
