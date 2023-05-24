from __future__ import annotations
from dataclasses import dataclass
from typing import ClassVar
import base64

from .host import Host
from .connection import Connection
from .info import Info
from .exceptions import InvalidNodeException, AerospikeException

@dataclass
class PartitionParser:
    conn: Connection
    node: Node
    PARTITION_GENERATION_CMD: ClassVar[str] = "partition-generation"
    REPLICAS_ALL_CMD: ClassVar[str] = "replicas-all"

    async def update_partitions(self, partitions: dict):
        commands = [
            self.PARTITION_GENERATION_CMD,
            self.REPLICAS_ALL_CMD
        ]
        info_map = await Info.request(self.conn, commands)

        partition_generation = int(info_map[self.PARTITION_GENERATION_CMD])

        if self.REPLICAS_ALL_CMD not in info_map:
            raise AerospikeException(f"{self.REPLICAS_ALL_CMD} was not returned from node {self.node}")

        replicas_all_response = info_map[self.REPLICAS_ALL_CMD]
        rows = replicas_all_response.split(";")
        for row in rows:
            entries = row.split(":,")
            namespace = entries[0]
            replica_count = int(entries[1])

            if namespace not in partitions:
                partitions[namespace] = {}

            for i in range(replica_count):
                bitmap = entries[2 + i]
                bitmap = base64.b64decode(bitmap)

class Cluster:
    def __init__(self, hosts: list[Host]):
        self.seeds = hosts
        self.nodes: list[Node] = []
        self.partition_map = {}

        self.min_conns_per_node = 10 # for testing
        self.conn_timeout = 3 # for testing

    async def tend(self):
        # Clear node reference counts
        for node in self.nodes:
            node.ref_count = 0
            node.partition_changed = False

        if len(self.nodes) == 0:
            # No active nodes
            await self.seed_nodes()

        for node in self.nodes:
            if node.partition_changed:
                node.refresh_partitions()

    async def seed_nodes(self):
        self.nodes = []
        for seed in self.seeds:
            # Get peers of this seed
            try:
                seed_nv = await NodeValidator.new(self, seed)
            except Exception as e:
                # If this seed fails, try another seed
                print(f"Failed to seed node: {e}")
                continue

            seed_node = await Node.new(self, seed_nv)
            self.nodes.append(seed_node)

            # Validate each peer
            for peer in seed_nv.peers:
                try:
                    peer_nv = await NodeValidator.new(self, peer)
                except Exception as e:
                    print(f"Failed to seed node: {e}")
                    continue

                node_names = [node.name for node in self.nodes]
                if peer_nv.name in node_names:
                    # We already found this node before
                    continue

                peer_node = Node(self, peer_nv)
                self.nodes.append(peer_node)

class Node:
    def __init__(self, cluster: Cluster, nv: NodeValidator):
        self.conns = []
        # The cluster this node belongs to
        self.cluster = cluster
        # Actually create a node from the data that NodeValidator collected from the ip address
        self.name = nv.name
        self.peers = nv.peers
        self.features = nv.features
        self.host = nv.host

        self.ref_count = 0
        self.partition_changed = True

    @staticmethod
    async def new(cluster: Cluster, nv: NodeValidator):
        node = Node(cluster, nv)
        await node.create_min_connections()
        return node

    async def create_min_connections(self):
        for _ in range(self.cluster.min_conns_per_node):
            conn = Connection.new(self.host.name, self.host.port, self.cluster.conn_timeout)
            self.conns.append(conn)

@dataclass
class NodeValidator:
    # The cluster class stores the connection timeout to use here
    # The timeout can be updated, so we need the cluster object to get the latest timeout dynamically
    cluster: Cluster
    host: Host
    features: dict[str, bool] = dict.fromkeys([
        "pscan",
        "query-show",
        "batch-any",
        "pquery"
    ], False)
    peers: list[Host] = []

    @staticmethod
    async def new(cluster: Cluster, host: Host):
        nv = NodeValidator(cluster, host)
        nv.peers = await nv.validate_and_get_peers()
        return nv

    async def validate_and_get_peers(self) -> list[Host]:
        conn = await Connection.new(self.host.name, self.host.port, self.cluster.conn_timeout)
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
        except:
            raise
        finally:
            await conn.close()
        return peers

    def set_features(self, responses: dict[str, str]):
        # TODO: what if features isn't in response?
        features_response = responses["features"].split(";")
        for feature in self.features:
            if feature in features_response:
                self.features[feature] = True

        if self.features["has_partition_scan"] is False:
            raise AerospikeException(f"Node {self.name} {self.host} version < 4.9. " \
                                     "This client requires server version >= 4.9")
