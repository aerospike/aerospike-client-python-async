from __future__ import annotations
from dataclasses import dataclass
import ipaddress
import socket

from .host import Host
from .connection import Connection
from .info import Info, InfoPolicy
from .exceptions import InvalidNodeException, AerospikeException

# TODO

class Cluster:
    def __init__(self, hosts: list[Host]):
        self.seeds = hosts
        self.nodes: list[Node] = []

    async def tend(self):
        await self.refresh_nodes()

    async def refresh_nodes(self):
        if len(self.nodes) == 0:
            await self.seed_nodes()

        # Reset all node's ref counts
        for node in self.nodes:
            node.ref_count = 0

        # TODO: leave off
        for node in self.nodes:
            node.refresh()

    async def seed_nodes(self):
        self.nodes = []
        for seed in self.seeds:
            # Get peers of this seed
            try:
                seed_nv = await NodeValidator.new(seed)
            except Exception as e:
                # TODO: why aren't we throwing an error if seeding the node fails?
                print(f"Failed to seed node: {e}")
                continue

            seed_node = Node(seed_nv)
            self.nodes.append(seed_node)

            # Validate each peer
            for peer in seed_nv.peers:
                try:
                    peer_nv = await NodeValidator.new(peer)
                except Exception as e:
                    print(f"Failed to seed node: {e}")
                    continue

                node_names = [node.name for node in self.nodes]
                if peer_nv.name in node_names:
                    # We already found this node before
                    continue

                peer_node = Node(peer_nv)
                self.nodes.append(peer_node)

class Node:
    def __init__(self, nv: NodeValidator):
        self.host = nv.host
        self.name = nv.name
        self.features = nv.features
        self.peers = nv.peers
        self.ref_count = 0
        # TODO: leave off
        self.conn_pool = 

@dataclass
class NodeFeatures:
    has_partition_scan: bool = False
    has_query_show: bool = False
    has_batch_any: bool = False
    has_partition_query: bool = False

@dataclass
class NodeValidator:
    host: Host
    features: NodeFeatures = NodeFeatures()
    peers: list[Host] = []

    @staticmethod
    async def new(host: Host):
        nv = NodeValidator(host)
        nv.peers = await nv.get_peers()
        return nv

    async def get_peers(self) -> list[Host]:
        # TODO: what should the timeout be?
        conn = await Connection.new(self.host.name, self.host.port, 1)
        commands = [
            "node",
            "features",
            "service-clear-std"
        ]
        try:
            info_map = await Info.request(conn, commands)

            # A valid Aerospike node must return a response for "node" and "features"
            if "node" not in info_map:
                raise InvalidNodeException(f"Host {self.host} did not return a node name!")
            self.name = info_map["node"]

            self.set_features(info_map)

            # Converts peers string to list of hosts
            peers = info_map["service-clear-std"].split(",")
            peers = [peer.split(":") for peer in peers]
            peers = [Host(peer[0], int(peer[1])) for peer in peers]
        # TODO: in Ruby code, the exception is eaten
        except:
            raise
        finally:
            await conn.close()
        return peers

    def set_features(self, responses: dict[str, str]):
        if "features" not in responses:
            raise InvalidNodeException(f"Host {self.host} did not return a list of features!")

        features = responses["features"].split(";")
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
