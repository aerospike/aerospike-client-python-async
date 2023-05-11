from __future__ import annotations
from dataclasses import dataclass
import ipaddress
import socket

from .host import Host
from .connection import Connection
from .info import Info, InfoPolicy
from .exceptions import InvalidNodeException, AerospikeException

# TODO

@dataclass
class NodeFeatures:
    has_partition_scan: bool = False
    has_query_show: bool = False
    has_batch_any: bool = False
    has_partition_query: bool = False

class Cluster:
    def __init__(self, hosts: list[Host]):
        self.seeds = hosts
        self.nodes = []

    def tend(self):
        self.refresh_nodes()

    def refresh_nodes(self):
        if len(self.nodes) == 0:
            self.seed_nodes()

    async def seed_nodes(self):
        nodes = []
        for seed in self.seeds:
            try:
                seed_nv = NodeValidator()
                await seed_nv.seed_node(self, seed)
            except Exception as e:
                print(f"Failed to seed node: {e}")
                continue

            for alias in seed_nv.aliases:
                if alias == seed:
                    nv = seed_nv
                else:
                    try:
                        nv = NodeValidator()
                        await nv.seed_node(self, alias)
                    except Exception as e:
                        print(f"Failed to seed node: {e}")
                        continue

                node_names = [node.name for node in nodes]
                if nv.name in node_names:
                    # We already found this node before
                    continue

                # TODO: leave off for later
                node = Node()

class Node:
    pass

class NodeValidator:
    @staticmethod
    def is_ip(hostname: str) -> bool:
        try:
            ipaddress.IPv4Address(hostname)
            ipaddress.IPv6Address(hostname)
        except ValueError:
            # Failed to parse IP
            return False
        return True

    # Find all ip addresses under this host name
    @staticmethod
    def resolve(hostname: str) -> list[str]:
        if NodeValidator.is_ip(hostname):
            return [hostname]

        # DNS resolution
        _, _, ip_addrs = socket.gethostbyname_ex(hostname)
        return ip_addrs

    @staticmethod
    def is_loopback(address: str) -> bool:
        ip_addr = ipaddress.ip_address(address)
        return ip_addr.is_loopback

    async def get_hosts(self, address: str) -> list[Host]:
        # Include this host itself
        # TODO: why set this as the default?
        aliases = [Host(address, self.host.port)]

        # TODO: just reuse connection class for now
        # Not sure if this is the right way though
        conn = await Connection.new(None, address, self.host.port)
        try:
            commands = [
                "node",
                "features"
            ]
            if self.is_loopback(address) is False:
                # TODO: doesn't check if cluster tls is enabled
                service_cmd = commands.append("service-clear-std")

            # TODO: need to create default info policy
            policy = InfoPolicy(1)
            info_map = await Info.request(policy, conn, commands)

            if "node" not in info_map:
                raise InvalidNodeException("Node name is null")
            self.name = info_map["node"]

            self.set_features(info_map)

            # This assumes a loopback address only points to a one node cluster
            if self.is_loopback(address) is False:
                # Converts peers string to list of hosts
                peers = info_map[service_cmd].split(",")
                peers = [peer.split(":") for peer in peers]
                aliases = [Host(peer[0], int(peer[1])) for peer in peers]
        except:
            # TODO: why eat the exception
            await conn.close()
        return aliases

    async def seed_node(self, cluster: Cluster, host: Host):
        self.cluster = cluster
        self.host = host
        self.aliases = []

        addresses = self.resolve(host.name)
        for address in addresses:
            aliases = await self.get_hosts(address)
            self.aliases.extend(aliases)

    def set_features(self, responses: dict[str, str]):
        features = responses.get("features")
        # TODO: What happens if features doesn't exist in node?
        features = features.split(";")
        # TODO: node features must be assigned to node
        self.node_fts = NodeFeatures()
        if "pscans" in features:
            self.node_fts.has_partition_scan = True
        if "query-show" in features:
            self.node_fts.has_query_show = True
        if "batch-any" in features:
            self.node_fts.has_batch_any = True
        if "pquery" in features:
            self.node_fts.has_partition_query = True

        if self.node_fts.has_partition_scan is False:
            # TODO: assuming this is a client exception?
            raise AerospikeException("Node {self.name} {self.primary_host} version < 4.9. " \
                                     "This client requires server version >= 4.9")
