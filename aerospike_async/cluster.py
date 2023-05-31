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
        self.nodes_map: dict[str, Node] = {}
        self.partition_map = {}
        self.tend_count = 0

        self.min_conns_per_node = 10 # for testing
        self.conn_timeout = 3 # for testing
        self.max_error_rate = 100
        self.error_rate_window = 1

    async def tend(self):
        peers = Peers()

        # Clear node reference counts
        for node in self.nodes:
            node.ref_count = 0
            node.partition_changed = False
            node.rebalance_changed = False

        if len(self.nodes) == 0:
            # No active nodes
            await self.seed_nodes(peers)
        else:
            for node in self.nodes:
                await node.refresh(peers)

            if peers.generation_changed:
                peers.refresh_count = 0

                for node in self.nodes:
                    await node.refresh_peers(peers)

                remove_list = self.find_nodes_to_remove(peers.refresh_count)

                if len(remove_list) > 0:
                    await self.remove_nodes(remove_list)

            if len(peers.nodes) > 0:
                self.add_nodes(peers.nodes)
                self.refresh_peers(peers)

        self.invalid_node_count = len(peers.invalid_hosts)

        for node in self.nodes:
            if node.partition_changed:
                await node.refresh_partitions(peers)

        self.tend_count += 1

        if self.tend_count % 30 == 0:
            for node in self.nodes:
                await node.balance_connections()

        if self.max_error_rate > 0 and self.tend_count % self.error_rate_window == 0:
            for node in self.nodes:
                node.error_count = 0

    def refresh_peers(self, peers: Peers):
        pass

    def find_nodes_to_remove(self, refresh_count: int):
        nodes_to_remove = []
        for node in self.nodes:
            if not node.active:
                nodes_to_remove.append(node)

            if refresh_count == 0 and node.failures >= 5:
                nodes_to_remove.append(node)

            if len(self.nodes) > 1 and refresh_count >= 1 and node.ref_count == 0:
                if node.failures == 0:
                    if self.node_in_partition_map(node) is False:
                        nodes_to_remove.append(node)
                else:
                    nodes_to_remove.append(node)
        return nodes_to_remove

    async def remove_nodes(self, nodes: list[Node]):
        for node in nodes:
            del self.nodes_map[node.name]
            await node.close()

    def add_nodes(self, nodes: dict[str, Node]):
        pass

    def node_in_partition_map(self, node: Node) -> bool:
        for ns_partition_map in self.partition_map.values():
            for node_list in ns_partition_map.values():
                if node in node_list:
                    return True
        return False

    # TODO: result is not used
    async def seed_nodes(self, peers: Peers) -> bool:
        self.nodes = []
        nv = NodeValidator()
        for seed in self.seeds:
            try:
                node = await nv.seed_node(seed, self, peers)
                if node != None:
                    await self.add_seed_and_peers(node, peers)
                    return True
            except Exception as e:
                peers.invalid_hosts.add(seed)
                # If this seed fails, try another seed
                print(f"Failed to seed node: {e}")
                continue

        if nv.fallback != None:
            peers.refresh_count = 1
            await self.add_seed_and_peers(nv.fallback, peers)

        return False

    async def add_seed_and_peers(self, seed: Node, peers: Peers):
        await seed.create_min_connections()
        self.nodes_map.clear()
        node_array = []

        self.nodes_map[seed.name] = seed
        node_array.append(seed)

        for peer in peers.nodes.values():
            self.nodes_map[peer.name] = peer
            node_array.append(peer)

        self.nodes = node_array

class PeerParser:
    def parse(self, info_response: str) -> list[Peer]:
        RESPONSE_REGEX = re.compile(r"(\d+), (\d+), (.*)")
        match = RESPONSE_REGEX.search(info_response)
        if match is None:
            raise AerospikeException("Unable to parse peers")
        self.peers_generation, default_port, peers = match.groups()
        default_port = int(default_port)

        PEER_REGEX = re.compile(r"\[(\d*), (\d*), \[(?:[\d\.]+(?:, )?)+\]\]")
        peer_matches = PEER_REGEX.finditer(peers)
        peers = []
        for peer_match in peer_matches:
            node_name, _, peer_ips = peer_match.groups()
            peer_ips = peer_ips.split(",")
            hosts = [Host(ip, default_port, None) for ip in peer_ips]
            peer = Peer(node_name, hosts)
            peers.append(peer)
        return peers

class Node:
    def __init__(self, cluster: Cluster, nv: NodeValidator):
        self.cluster = cluster
        self.conns = []
        # Actually create a node from the data that NodeValidator collected from the ip address
        self.name = nv.name
        self.features = nv.features
        self.host = nv.host
        self.tend_conn = nv.conn

        self.ref_count = 0
        self.peers_generation = -1
        self.partition_generation = -1
        self.rebalance_generation = -1
        self.generation_changed = True
        self.partition_changed = True
        self.rebalance_changed = False

        self.peers_count = 0

        self.health = 100
        self.active = True
        self.responded = False
        self.failures = 0
        self.error_count = 0

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

        try:
            info_map = await Info.request(self.tend_conn, commands)
            if len(info_map) == 0:
                raise AerospikeException(f"Unable to fetch peers from node {self.name}")

            response = info_map["peers-clear-std"]
            peers.peers.clear()
            parser = PeerParser()
            peers.peers = parser.parse(response)

            peers_validated = True
            for peer in peers.peers:
                if self.peer_node_exists(peers, peer.node_name) is False:
                    continue

                node_validated = False
                for host in peer.hosts:
                    if host in peers.invalid_hosts:
                        continue

                    try:
                        nv = await NodeValidator.new(host, cluster.conn_timeout)
                        if peer.node_name != nv.name:
                            print(f"For host {host}, peer node {peer.node_name} is different from actual node {nv.name}")
                            if self.peer_node_exists(peers, nv.name):
                                await nv.tend_conn.close()
                                node_validated = True
                                break

                        node = await Node.new(cluster, nv)
                        peers.nodes[nv.name] = node
                        node_validated = True
                        break
                    except Exception as e:
                        peers.invalid_hosts.add(host)
                        print(f"Add host {host} failed: {e}")

                if node_validated == False:
                    peers_validated = False

            if peers_validated:
                self.peers_generation = parser.peers_generation
            peers.refresh_count += 1
        except Exception as e:
            self.failures += 1
            await self.tend_conn.close()
            print(f"Node {self.name} refresh failed: {e}")

    def peer_node_exists(self, peers: Peers, node_name: str) -> bool:
        node = self.cluster.nodes_map.get(node_name)
        if node != None:
            node.ref_count += 1
            return True

        node = peers.nodes.get(node_name)
        if node != None:
            node.ref_count += 1
            return True

        return False

    def should_refresh(self):
        return self.failures == 0 and self.active

    async def create_min_connections(self):
        for _ in range(self.cluster.min_conns_per_node):
            conn = await Connection.new(self.host.name, self.host.port, self.cluster.conn_timeout)
            self.conns.append(conn)

    async def refresh_partitions(self, peers: Peers):
        parser = PartitionParser()

    async def close(self):
        self.active = False
        await self.tend_conn.close()
        for conn in self.conns:
            await conn.close()

    async def balance_connections(self):
        pass

class NodeValidator:
    peers: list[Host]
    host: Host
    timeout_secs: float
    fallback: Node

    async def seed_node(self, host: Host, cluster: Cluster, peers: Peers) -> Node:
        is_ip = host.is_ip()
        if not is_ip:
            raise AerospikeException("Invalid host passed to node validator")

        try:
            await self.validate_address(cluster, host)
            node = Node(cluster, self)
            res = await self.validate_peers(node, peers)
            if res == True:
                return node
        except:
            raise
        finally:
            await conn.close()

    async def validate_address(self, cluster, host: Host):
            self.conn = await Connection.new(host.name, host.port, cluster.conn_timeout)
            commands = [
                "node",
                "partition-generation",
                "features",
            ]
            if host.is_loopback() is False:
                commands.append("service-clear-std")

            info_map = await Info.request(self.conn, commands)

            # A valid Aerospike node must return a response for "node" and "features"

            if "node" not in info_map:
                raise InvalidNodeException(f"Host {host} did not return a node name!")
            self.name = info_map["node"]

            if "features" not in info_map:
                raise InvalidNodeException(f"Host {host} did not return a list of features!")
            features = info_map["features"]
            self.features = features.split(";")

    async def validate_peers(self, node: Node, peers: Peers) -> bool:
        if peers is None:
            return True

        # Get node's peers for the first time
        peers.refresh_count = 0
        try:
            await node.refresh_peers(peers)
        except Exception as e:
            await node.close()
            raise e

        if node.peers_count == 0:
            if self.fallback == None:
                self.fallback = node
            else:
                await node.close()
            return False

        if self.fallback != None:
            print("Skip orphan node")
            self.fallback.close()
            self.fallback = None

        return True
