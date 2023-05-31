from __future__ import annotations
from dataclasses import dataclass, field
from typing import ClassVar
import base64
import re

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

                for byte in bitmap:
                    for i in range(8):
                        contains_partition = byte & (0b10000000 >> i) != 0
                        if contains_partition

@dataclass
class Peer:
    node_name: str
    hosts: list[Host]

@dataclass
class Peers:
    peers: list[Peer] = []
    nodes: dict[str, Node] = {}
    invalid_hosts: set[Host] = set()
    refresh_count = 0
    generation_changed = False

class Cluster:
    def __init__(self, hosts: list[Host]):
        self.seeds = hosts
        self.nodes: list[Node] = []
        self.partition_map = {}
        self.tend_count = 0

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
            node.reset()

        peers = Peers()

        for node in self.nodes:
            await node.refresh(peers)

        if peers.generation_changed:
            peers.refresh_count = 0

            for node in self.nodes:
                await node.refresh_peers(peers)

        for node in self.nodes:
            if node.partition_changed:
                await node.refresh_partitions()

        self.tend_count += 1

        for node in self.nodes:
            await node.balance_connections()


    async def seed_nodes(self):
        self.nodes = []
        for seed in self.seeds:
            try:
                seed_nv = await NodeValidator.new(seed, self.conn_timeout)
            except Exception as e:
                # If this seed fails, try another seed
                print(f"Failed to seed node: {e}")
                continue

            seed_node = await Node.new(self, seed_nv)
            self.nodes.append(seed_node)

            # # Validate each peer
            # for peer in seed_nv.peers:
            #     try:
            #         peer_nv = await NodeValidator.new(peer, self.conn_timeout)
            #     except Exception as e:
            #         print(f"Failed to seed node: {e}")
            #         continue

            #     node_names = [node.name for node in self.nodes]
            #     if peer_nv.name in node_names:
            #         # We already found this node before
            #         continue

            #     peer_node = Node(self, peer_nv)
            #     self.nodes.append(peer_node)

class PeerParser:
    @staticmethod
    def parse(info_response: str):
        RESPONSE_REGEX = re.compile(r"(\d+), (\d+), [(.*)]")
        match = RESPONSE_REGEX.search(info_response)
        if match is None:
            raise AerospikeException("Unable to parse peers")
        peers_generation, default_port, peers = match.groups()

        PEERS_REGEX = re.compile(r"[\[(.*)\],?]+")
        peers = PEERS_REGEX.findall(peers)
        for peer in peers:
            pass
            # TODO

class Node:
    def __init__(self, cluster: Cluster, nv: NodeValidator):
        self.cluster = cluster
        self.conns = []
        # Actually create a node from the data that NodeValidator collected from the ip address
        self.name = nv.name
        self.features = nv.features
        self.host = nv.host
        self.tend_conn = nv.tend_conn

        self.ref_count = 0
        self.peers_generation = -1
        self.partition_generation = -1
        self.rebalance_generation = -1
        self.generation_changed = True
        self.partition_changed = True
        self.rebalance_changed = False

        self.health = 100
        self.active = True
        self.responded = False
        self.failures = 0

    @staticmethod
    async def new(cluster: Cluster, nv: NodeValidator):
        node = Node(cluster, nv)
        await node.create_min_connections()
        return node

    def reset(self):
        self.ref_count = 0
        self.partition_changed = False

    async def refresh(self, peers: Peers):
        conn = self.tend_conn
        try:
            commands = [
                "node",
                "partition-generation",
                "cluster-name",
                "peers-generation",
                "services",
                "rebalance-generation"
            ]
            info_map = await Info.request(conn, commands)

            self.refresh_peers_generation(info_map, peers)
            self.refresh_partition_generation(info_map)
            self.refresh_rebalance_generation(info_map)

            if "node" not in info_map:
                self.health -= 1
                raise AerospikeException("Node name is empty")

            if info_map["node"] != self.name:
                self.active = False
                raise AerospikeException(f"Node name changed from {self.name} to {info_map['node']}")

            self.health = 100
            self.responded = True

            peers.refresh_count += 1
            self.failures = 0
        except Exception as e:
            await conn.close()
            self.health -= 1
            peers.generation_changed = True
            print(f"Node {self.name} refresh failed: {e}")
            self.failures += 1

    def refresh_peers_generation(self, info_map: dict[str, str], peers: Peers):
        if "peers-generation" not in info_map:
            raise AerospikeException("peers-generation is empty")
        peers_gen = int(info_map["peers-generation"])
        if peers_gen != self.peers_generation:
            peers.generation_changed = True


    def refresh_rebalance_generation(self, info_map: dict[str, str]):
        if "rebalance-generation" not in info_map:
            raise AerospikeException("rebalance-generation is empty")
        rebalance_gen = int(info_map["rebalance-generation"])
        if rebalance_gen != self.rebalance_generation:
            self.rebalance_changed = True
            self.rebalance_generation = rebalance_gen

    def refresh_partition_generation(self, info_map: dict[str, str]):
        if "partition-generation" not in info_map:
            raise AerospikeException("partition-generation is empty")

        partition_gen = int(info_map["partition-generation"])
        if partition_gen != self.partition_generation:
            self.partition_changed = True
            self.partition_generation = partition_gen

    async def refresh_peers(self, peers: Peers):
        if self.should_refresh() is False:
            return
        print(f"Update peers for node {self.name}")

        cluster = self.cluster
        commands = [
            "peers-clear-std"
        ]
        info_map = await Info.request(self.tend_conn, commands)
        if len(info_map) == 0:
            raise AerospikeException(f"Unable to fetch peers from node {self.name}")

        response = info_map["peers-clear-std"]
        peers.peers.clear()
        PeerParser.parse(response)

    def should_refresh(self):
        return self.failures == 0 and self.active

    async def create_min_connections(self):
        for _ in range(self.cluster.min_conns_per_node):
            conn = await Connection.new(self.host.name, self.host.port, self.cluster.conn_timeout)
            self.conns.append(conn)

    # async def refresh_partitions(self):
    #     parser = PartitionParser()

class NodeValidator:
    name: str
    features: list[str]
    peers: list[Host]
    host: Host
    timeout_secs: float
    tend_conn: Connection

    def __init__(self, name: str, features: list[str], tend_conn: Connection):
        self.name = name
        self.features = features
        self.tend_conn = tend_conn

    @staticmethod
    async def new(host: Host, timeout_secs: float) -> NodeValidator:
        is_ip = host.is_ip()
        if not is_ip:
            raise AerospikeException("Invalid host passed to node validator")

        conn = await Connection.new(host.name, host.port, timeout_secs)

        try:
            commands = [
                "node",
                "features",
            ]
            if host.is_loopback() is False:
                commands.append("service-clear-std")

            info_map = await Info.request(conn, commands)

            # A valid Aerospike node must return a response for "node" and "features"

            if "node" not in info_map:
                raise InvalidNodeException(f"Host {host} did not return a node name!")
            name = info_map["node"]

            if "features" not in info_map:
                raise InvalidNodeException(f"Host {host} did not return a list of features!")
            features = info_map["features"]
            features = features.split(";")
        except:
            raise
        finally:
            await conn.close()

        return NodeValidator(name, features, conn)
