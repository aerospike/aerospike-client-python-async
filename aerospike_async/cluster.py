from __future__ import annotations
import logging
from dataclasses import dataclass, field
from typing import ClassVar
import base64
import asyncio
import time
from typing import Optional

from .host import Host
from .connection import Connection
from .info import Info
from .exceptions import InvalidNodeException, AerospikeException

class Partitions:
    replicas: list[list[Optional[Node]]]
    regimes: list[int]

    # TODO: missing sc mode
    def __init__(self, partition_count: int, replica_count: int):
        self.replicas = []
        for _ in range(replica_count):
            # Insert partition array for each replica
            self.replicas.append([None for _ in range(partition_count)])
        self.regimes = [0 for _ in range(partition_count)]

    @staticmethod
    def resize(other: Partitions, replica_count: int) -> Partitions:
        replicas = []

        if len(other.replicas) < replica_count:
            # Copy existing entries
            for i in range(len(other.replicas)):
                replicas.append(other.replicas[i])

            # Create new entries
            for i in range(len(other.replicas), replica_count):
                replicas.append([None for _ in range(len(other.regimes))])
        else:
            # Copy existing entries
            for i in range(replica_count):
                replicas.append(other.replicas[i])

        # TODO: create another constructor
        partitions = Partitions(len(other.regimes), replica_count)
        partitions.replicas = replicas
        partitions.regimes = other.regimes
        return partitions

class PartitionParser:
    conn: Connection
    node: Node
    partition_map: dict[str, Partitions]
    partition_count: int
    generation: int
    info: Info

    PARTITION_GENERATION_COMMAND: ClassVar[str] = "partition-generation"
    REPLICAS_ALL_COMMAND: ClassVar[str] = "replicas"

    def __init__(self, partition_map: dict[str, Partitions], partition_count: int):
        self.partition_count = partition_count
        self.partition_map = partition_map
        self.copied = False
        self.regime_error = False

    @classmethod
    async def new(cls, conn: Connection, node: Node, partition_map: dict[str, Partitions], partition_count: int) -> PartitionParser:
        pp = PartitionParser(partition_map, partition_count)
        commands = [
            cls.PARTITION_GENERATION_COMMAND,
            cls.REPLICAS_ALL_COMMAND
        ]
        pp.info = await Info.new(conn, commands)

        if pp.info.length == 0:
            raise AerospikeException("Partition info is empty")

        pp.generation = pp.parse_generation()
        pp.parse_replicas_all(node, cls.REPLICAS_ALL_COMMAND)
        return pp

    def parse_generation(self) -> int:
        self.info.parse_name(self.PARTITION_GENERATION_COMMAND)
        gen = self.info.parse_int()
        self.info.expect('\n')
        return gen

    def parse_replicas_all(self, node: Node, command: str):
        info = self.info
        self.info.parse_name(command)

        begin = info.offset
        regime = 0

        while info.offset < info.length:
            if info.buffer[info.offset] == ':':
                # Parse namespace
                namespace = str(info.buffer[begin:info.offset], encoding='utf-8').strip()
                if len(namespace) <= 0 or len(namespace) >= 32:
                    # TODO: get truncated response
                    response = str(info.buffer, encoding='utf-8')
                    raise AerospikeException(f"Invalid partition namespace {namespace}. Response={response}")

                info.offset += 1
                begin = info.offset

                # Parse regime
                if command == self.REPLICAS_ALL_COMMAND:
                    while info.offset < info.length:
                        b = info.buffer[info.offset]

                        if b == ',':
                            break
                        info.offset += 1
                    regime = int.from_bytes(info.buffer[begin:info.offset], byteorder='big')
                    info.offset += 1
                    begin = info.offset

                # Parse replica count
                while info.offset < info.length:
                    b = info.buffer[info.offset]

                    if b == ',':
                        break
                    info.offset += 1
                replica_count = int.from_bytes(info.buffer[begin:info.offset], byteorder='big')

                partitions = self.partition_map.get(namespace)

                if partitions == None:
                    # Create new replica array
                    partitions = Partitions(self.partition_count, replica_count)
                    # TODO: not sure what this is for?
                    self.copy_partition_map()
                    self.partition_map[namespace] = partitions
                elif len(partitions.replicas) != replica_count:
                    logging.info(f"Namespace {namespace} replication factor changed from {len(partitions.replicas)} to {replica_count}")

                    tmp = Partitions.resize(partitions, replica_count)
                    self.copy_partition_map()
                    partitions = tmp
                    self.partition_map[namespace] = partitions

                # Parse partition bitmaps
                for i in range(replica_count):
                    info.offset += 1
                    begin = info.offset

                    # Find bitmap endpoint
                    while info.offset < info.length:
                        b = info.buffer[info.offset]

                        if b == ',' or b == ';':
                            break

                        info.offset += 1

                    if info.offset == begin:
                        response = str(info.buffer, encoding='utf-8')
                        raise AerospikeException(f"Empty partition id for namespace {namespace}. Response={response}")

                    self.decode_bitmap(node, partitions, i, regime, begin)
                info.offset += 1
                begin = info.offset
            else:
                info.offset += 1

    def decode_bitmap(self, node: Node, partitions: Partitions, index: int, regime: int, begin: int):
        node_array = partitions.replicas[index]
        regimes = partitions.regimes

        info = self.info
        restore_buffer = base64.decodebytes(info.buffer[begin:info.offset])

        for i in range(self.partition_count):
            node_old = node_array[i]
            # TODO: how does this work?
            if restore_buffer[i >> 3] & (0x80 >> (i & 7)) != 0:
                # Node owns this partition
                regime_old = regimes[i]
                if regime >= regime_old:
                    if regime > regime_old:
                        regimes[i] = regime

                    if node_old != None and node_old != node:
						# Force previously mapped node to refresh it's partition map on next cluster tend.
                        node_old.partition_generation = -1

                    node_array[i] = node
                else:
                    if self.regime_error is False:
                        logging.info(f"{str(node)} regime({regime}) < old regime({regime_old})")
                        self.regime_error = True

    def copy_partition_map(self):
        if self.copied is False:
            self.partition_map = self.partition_map.copy()
            self.copied = True

@dataclass
class Peer:
    node_name: str
    hosts: list[Host]

@dataclass
class Peers:
    peers: list[Peer] = field(default_factory=list)
    nodes: dict[str, Node] = field(default_factory=dict)
    invalid_hosts: set[Host] = field(default_factory=set)
    refresh_count = 0
    generation_changed = False

class Cluster:
    cluster_name: Optional[str]

    def __init__(self, hosts: list[Host]):
        # Initial host nodes specified by user
        self.seeds = hosts
        # Active nodes in cluster
        self.nodes: list[Node] = []
        # Map of active nodes in cluster
        # Only accessed within cluster tend thread
        self.nodes_map: dict[str, Node] = {}
        # Hints for best node for a partition
        self.partition_map = {}
        # Cluster tend counter
        self.tend_count = 0
        self.tend_valid = True

        # Expected cluster name
        self.cluster_name = None

        # Minimum sync connections per node
        self.min_conns_per_node = 10
        # Initial connection timeout
        self.conn_timeout = 3
        # Max errors per node per error_rate_window
        self.max_error_rate = 100
        # Number of tend iterations defining window for max_error_rate
        self.error_rate_window = 1
        # Maximum socket idle to trim peak connections to min connections.
        # 55 seconds
        self.max_socket_idle_nanos_trim = 55 * 10**9
        # Interval in seconds between cluster tends
        self.tend_interval = 1
        # Does cluster support query by partition
        self.has_partition_query = False

    @staticmethod
    async def new(hosts: list[Host]):
        cluster = Cluster(hosts)
        await cluster.init_tend_thread()

    # TODO: add fail if not connected parameter
    async def init_tend_thread(self):
        await self.wait_till_stabilized()

        for host in self.seeds:
            logging.debug(f"Add seed {host}")

        # Add other nodes as seeds if they don't already exist
        seeds_to_add = []
        for node in self.nodes:
            host = node.host
            if self.find_seed(host) is False:
                seeds_to_add.append(host)

        if len(seeds_to_add) > 0:
            self.add_seeds(seeds_to_add)

        # Run cluster tend coroutine
        self.tend_valid = True
        tend = self.tend_coroutine()
        self.tend_thread = asyncio.create_task(tend)
        self.tend_thread.set_name("tend")
        await self.tend_thread

    async def tend_coroutine(self):
        while self.tend_valid:
            try:
                await self.tend()
            except Exception as e:
                logging.warn(f"Cluster tend failed: {e}")
            await asyncio.sleep(self.tend_interval)

    def add_seeds(self, hosts: list[Host]):
        seed_array = []

        for host in hosts:
            logging.debug(f"Add seed {host}")
            seed_array.append(host)

        self.seeds = seed_array

    def find_seed(self, search: Host):
        for seed in self.seeds:
            if seed == search:
                return True
        return False

    # Tend the cluster until it has stabilized and return control.
    # This helps avoid initial database request timeout issues when
    # a large number of threads are initiated at client startup.
    async def wait_till_stabilized(self):
        # TODO: pass in fail_if_not_connected and is_init
        await self.tend()

        if len(self.nodes) == 0:
            # TODO: fail_if_not_connected
            message = "Cluster seed(s) failed"
            logging.warn(message)

    async def tend(self):
        # My notes:
        # Pass this to each node
        # so every node sees changes
        # From client's perspective, every node knows about the changes in the whole cluster
        # even if some nodes don't actually know about the change (e.g due to network latency)
        # Java:
        # All node additions/deletions are performed in tend thread.
        # Initialize tend iteration node statistics.
        peers = Peers()

        # Clear node reference counts
        for node in self.nodes:
            node.reference_count = 0
            node.partition_changed = False
            node.rebalance_changed = False

        # If active nodes don't exist, seed cluster.
        if len(self.nodes) == 0:
            # No active nodes
            # TODO: implement fail_if_not_connected
            await self.seed_nodes(peers)
        else:
            # Check if there were any changes in the cluster
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
                await self.refresh_peers(peers)

        self.invalid_node_count = len(peers.invalid_hosts)

        for node in self.nodes:
            if node.partition_changed:
                await node.refresh_partitions(peers)

            # TODO
            # if node.rebalance_changed:
            #     node.refresh_racks()

        self.tend_count += 1

        # Balance connections every 30 tend iterations
        if self.tend_count % 30 == 0:
            for node in self.nodes:
                await node.balance_connections()

                # TODO: balance async connections

        if self.max_error_rate > 0 and self.tend_count % self.error_rate_window == 0:
            for node in self.nodes:
                node.error_count = 0

        # TODO: process recover queue

    async def refresh_peers(self, peers: Peers):
        while True:
            nodes = list(peers.nodes.values()).copy()
            peers.nodes.clear()

            for node in nodes:
                await node.refresh_peers(peers)

            if len(peers.nodes) > 0:
                self.add_nodes(peers.nodes)
            else:
                break

    async def create_node(self, nv: NodeValidator):
        node = Node(self, nv)
        await node.create_min_connections()
        return node

    def find_nodes_to_remove(self, refresh_count: int):
        remove_list = []

        for node in self.nodes:
            if not node.active:
                # Inactive nodes must be removed
                remove_list.append(node)
                continue

            if refresh_count == 0 and node.failures >= 5:
				# All node info requests failed and this node had 5 consecutive failures.
				# Remove node.  If no nodes are left, seeds will be tried in next cluster
				# tend iteration.
                remove_list.append(node)
                continue

            if len(self.nodes) > 1 and refresh_count >= 1 and node.reference_count == 0:
				# Node is not referenced by other nodes.
				# Check if node responded to info request.
                if node.failures == 0:
                    # Node is alive, but not referenced by other nodes.  Check if mapped.
                    if self.node_in_partition_map(node) is False:
						# Node doesn't have any partitions mapped to it.
						# There is no point in keeping it in the cluster.
                        remove_list.append(node)
                else:
                    # Node is not responding. remove it
                    remove_list.append(node)

        return remove_list

    async def remove_nodes(self, nodes: list[Node]):
        for node in nodes:
            del self.nodes_map[node.name]
            # TODO: remove aliases
            await node.close()
        # TODO: remove nodes copy

    def add_nodes(self, nodes_to_add: dict[str, Node]):
        node_array = []
        for node in self.nodes:
            node_array.append(node)

        for node in nodes_to_add.values():
            node_array.append(node)
            self.add_node(node)

        self.has_partition_query = self.supports_partition_query(node_array)

        self.nodes = node_array

    @staticmethod
    def supports_partition_query(nodes: list[Node]):
        if len(nodes) == 0:
            return False

        for node in nodes:
            if node.has_partition_query() is False:
                return False
        return True

    def add_node(self, node: Node):
        logging.info(f"Add node {node}")
        self.nodes_map[node.name] = node
        # TODO: add node's aliases

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
                node = await nv.seed_node(self, seed, peers)
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

        # TODO: addNodes(seed, peers) is this code block
        self.nodes_map[seed.name] = seed
        node_array = []
        node_array.append(seed)
        for peer in peers.nodes.values():
            self.nodes_map[peer.name] = peer
            node_array.append(peer)
        self.nodes = node_array

        if len(peers.nodes) > 0:
            await self.refresh_peers(peers)

    def is_conn_current_trim(self, last_used: int) -> bool:
        return (time.time_ns() - last_used) <= self.max_socket_idle_nanos_trim

class PeerParser:
    cluster: Cluster
    parser: Info
    port_default: int
    generation: int

    def __init__(self, cluster: Cluster, peers: list[Peer], parser: Info, command: str):
        self.cluster = cluster
        self.parser = parser

        if parser.length == 0:
            raise AerospikeException(f"{command} response is empty")

        parser.skip_to_value()
        self.generation = parser.parse_int()
        parser.expect(',')
        self.port_default = parser.parse_int()
        parser.expect(',')
        parser.expect('[')

        # Reset peers
        peers.clear()

        # No peers for this node
        if parser.buffer[parser.offset] == ord(']'):
            return

        while True:
            peer = self.parse_peer()
            peers.append(peer)

            if parser.offset < parser.length and parser.buffer[parser.offset] == ord(','):
                parser.offset += 1
            else:
                break

    @staticmethod
    async def new(cluster: Cluster, conn: Connection, peers: list[Peer]) -> PeerParser:
        # TODO: check for tls policy and services alternate
        command = "peers-clear-std"
        parser = await Info.new(conn, [command])

        peer_parser = PeerParser(cluster, peers, parser, command)
        return peer_parser

    def parse_peer(self):
        self.parser.expect('[')
        node_name = self.parser.parse_string(',')
        self.parser.offset += 1
        # TODO: ignore tls name for now
        _ = self.parser.parse_string(',')
        self.parser.offset += 1
        hosts = self.parse_hosts()
        self.parser.expect(']')

        peer = Peer(node_name, hosts)
        return peer

    def parse_hosts(self) -> list[Host]:
        hosts = []
        self.parser.expect('[')
        if self.parser.buffer[self.parser.offset] == ord(']'):
            return hosts

        while True:
            host = self.parse_host()
            hosts.append(host)

            if self.parser.buffer[self.parser.offset] == ord(']'):
                self.parser.offset += 1
                return hosts

            self.parser.offset += 1

    def parse_host(self) -> Host:
        if self.parser.buffer[self.parser.offset] == ord('['):
            # Parse IPv6 address
            self.parser.offset += 1
            host = self.parser.parse_string(']')
            self.parser.offset += 1
        else:
            # Parse IPv4 address
            # Stop at : because there is a port after the address
            # Stop at , because there is another address
            # Stop at ] because this is the last address for this node
            host = self.parser.parse_string(':', ',', ']')

        # TODO: check for ip map

        if self.parser.offset < self.parser.length:
            b = self.parser.buffer[self.parser.offset]

            if b == ord(':'):
                self.parser.offset += 1
                port = self.parser.parse_int()
                return Host(host, port)

            if b == ord(',') or b == ord(']'):
                return Host(host, self.port_default)

        # The response is truncated if:
        # 1. We stopped at :, and there was no port
        # 2. We stopped at , and there was another expected host
        # 3. We stopped at ], and the response ends here without a closing ] for the list of nodes
        # TODO: return truncated response
        response = str(self.parser.buffer, encoding='utf-8')
        raise AerospikeException(f"Unterminated host in response: {response}")

class Pool:
    conns: list[Optional[Connection]]
    head: int
    tail: int
    size: int
    min_size: int
    total: int # total connections

    def __init__(self, min_size: int, max_size: int):
        self.min_size = min_size
        self.size = 0
        self.conns = [None for _ in range(max_size)]
        self.total = 0
        self.head = 0
        self.tail = 0

    def offer(self, conn: Connection):
        if conn == None:
            # TODO: use Python equivalent
            raise AerospikeException("NullPointerException")

        if len(self.conns) == self.size:
            return False

        self.conns[self.head] = conn
        if self.head + 1 == len(self.conns):
            self.head = 0
        self.size += 1
        return True

    # Return number of connections that might be closed
    def excess(self) -> int:
        return self.total - self.min_size

    def close_idle(self, node: Node, count: int):
        cluster = node.cluster

        while count > 0:
            # TODO: lock might be important?
            if self.size == 0:
                return

            conn = self.conns[self.tail]

            if cluster.is_conn_current_trim(conn.last_used_time_ns):
                return

            self.conns[self.tail] = None
            self.tail += 1
            if self.tail == len(self.conns):
                self.tail = 0
            self.size -= 1

            self.close_idle(node, conn)
            count -= 1

class Node:
    PARTITIONS = 4096
    def __init__(self, cluster: Cluster, nv: NodeValidator):
        self.cluster = cluster
        self.name = nv.name
        # self.aliases = nv.aliases
        self.host = nv.primary_host
        # self.address = nv.primary_address
        # Reserve one connection for tending in case connection pool is empty
        # i.e node is busy
        self.tend_connection = nv.primary_conn
        self.connection_pool: list[Connection] = []
        # self.session_token = nv.session_token
        # self.session_expiration = nv.session_expiration
        self.features = nv.features
        # TODO: following 3 vars need to be atomic
        self.conns_opened = 1
        # self.conns_closed = 0
        self.error_count = 0
        # self.error_count = 0
        self.peers_generation = -1
        self.partition_generation = -1
        self.rebalance_generation = -1
        self.partition_changed = True
        # TODO: check if cluster is rack aware
        self.rebalance_changed = False
        # self.racks = ...
        self.active = True

        self.peers_count = 0
        self.reference_count = 0
        self.failures = 0

        # TODO: initialize multiple connection pools properly
        self.pool = Pool(10, 20)
        self.conns_opened = 1
        self.conns_closed = 0

    def reset(self):
        self.reference_count = 0
        self.partition_changed = False

    async def refresh(self, peers: Peers):
        conn = self.tend_connection
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
                raise AerospikeException("Node name is empty")

            if info_map["node"] != self.name:
                self.active = False
                raise AerospikeException(f"Node name changed from {self.name} to {info_map['node']}")

            self.responded = True

            peers.refresh_count += 1
            self.failures = 0
        except Exception as e:
            await conn.close()
            peers.generation_changed = True
            print(f"Node {self.name} refresh failed: {e}")
            self.failures += 1

    def refresh_peers_generation(self, info_map: dict[str, str], peers: Peers):
        if "peers-generation" not in info_map:
            raise AerospikeException("peers-generation is empty")

        peers_gen = int(info_map["peers-generation"])
        if peers_gen != self.peers_generation:
            peers.generation_changed = True
            # TODO

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
		# Do not refresh peers when node connection has already failed during this cluster tend iteration.
        if self.failures > 0 or not self.active:
            return

        cluster = self.cluster
        try:
            # TODO: check if logging debug is enabled?
            logging.debug(f"Update peers for node {self.name}")

            parser = await PeerParser.new(self.cluster, self.tend_connection, peers.peers)
            self.peers_count = len(peers.peers)

            peers_validated = True

            for peer in peers.peers:
                if self.find_peer_node(peers, peer.node_name) is False:
                    # Node already exists. Do not even try to connect to hosts.
                    continue

                node_validated = False

                # Find first host that connects
                for host in peer.hosts:
					# Do not attempt to add a peer if it has already failed in this cluster tend iteration.
                    if host in peers.invalid_hosts:
                        continue

                    try:
                        # Attempt connection to host
                        nv = NodeValidator()
                        await nv.validate_node(cluster, host)

                        if peer.node_name != nv.name:
							# Must look for new node name in the unlikely event that node names do not agree.
                            logging.warn(f"Peer node {peer.node_name} is different from actual node {nv.name} for host {host}")

                            if self.find_peer_node(peers, nv.name):
								# Node already exists. Do not even try to connect to hosts.
                                await nv.primary_conn.close()
                                node_validated = True
                                break

                        # Create new node
                        node = await self.cluster.create_node(nv)
                        peers.nodes[nv.name] = node
                        node_validated = True
                        break
                    except Exception as e:
                        # TODO: use peers.fail(host)
                        peers.invalid_hosts.add(host)
                        logging.warn(f"Add node {host} failed: {e}")

                if node_validated == False:
                    peers_validated = False

            # Only set new peers generation if all referenced peers are added to the cluster.
            if peers_validated:
                self.peers_generation = parser.generation
            peers.refresh_count += 1
        except Exception as e:
            await self.refresh_failed(e)

    async def refresh_failed(self, e: Exception):
        self.failures += 1
        if self.tend_connection.is_closed() is False:
            await self.close_connection_on_error(self.tend_connection)

		# Only log message if cluster is still active.
        if self.cluster.tend_valid:
            logging.warn(f"Node {self.name} refresh failed: {e}")

    async def close_connection_on_error(self, conn: Connection):
        self.conns_closed += 1
        self.incr_error_count()
        await conn.close()

    def incr_error_count(self):
        if self.cluster.max_error_rate > 0:
            self.error_count += 1

    def find_peer_node(self, peers: Peers, node_name: str) -> bool:
        # Check global node map for existing cluster
        node = self.cluster.nodes_map.get(node_name)

        if node != None:
            node.reference_count += 1
            return True

        # Check local node map for this tend iteration
        node = peers.nodes.get(node_name)

        if node != None:
            node.reference_count += 1
            return True

        return False

    async def create_min_connections(self):
        # TODO: use multiple connection pools
        if self.pool.min_size > 0:
            await self.create_connections(self.pool, self.pool.min_size)

        # TODO: create async connections

    async def create_connections(self, pool: Pool, count: int):
        while count > 0:
            try:
                conn = await self.create_connection(pool)
            except Exception as e:
                logging.debug(f"Failed to create connection: {e}")
                return

            if self.pool.offer(conn):
                pool.total += 1
            else:
                await self.close_idle_connection(conn)
                break
            count -= 1

    # Close connection without incrementing error count
    async def close_idle_connection(self, conn: Connection):
        self.conns_closed += 1
        await conn.close()

    async def create_connection(self, pool: Pool) -> Connection:
        conn = await Connection.new(self.host.name, self.host.port, self.cluster.conn_timeout)
        self.conns_opened += 1
        # TODO: check if auth enabled
        return conn

    def __repr__(self):
        return repr(f"{self.name} {self.host}")

    async def refresh_partitions(self, peers: Peers):
        if self.failures > 0 or self.active is False or (self.peers_count == 0 and peers.refresh_count > 1):
            return

        try:
            logging.debug(f"Update partition map for node {self}")
            parser = await PartitionParser.new(self.tend_connection, self, self.cluster.partition_map, Node.PARTITIONS)

            if parser.copied:
                self.cluster.partition_map = parser.partition_map

            partition_generation = parser.generation
        except Exception as e:
            await self.refresh_failed(e)

    async def close(self):
        self.active = False
        await self.tend_connection.close()
        for conn in self.connection_pool:
            await conn.close()

    async def balance_connections(self):
        # TODO: balance multiple conn pools
        pool = self.pool
        excess = pool.excess()

        if excess > 0:
            pool.close_idle(self, excess)
        elif excess < 0 and self.error_count_within_limit():
            # We are below min number of connections
            await self.create_connections(pool, -excess)

    def error_count_within_limit(self):
        return self.cluster.max_error_rate <= 0 or self.error_count <= self.cluster.max_error_rate

    def has_partition_query(self):
        return "pquery" in self.features

class NodeValidator:
    peers: list[Host]
    primary_host: Host
    primary_conn: Optional[Connection]
    timeout_secs: float
    fallback: Optional[Node]
    name: str
    features: list[str]

    def __init__(self):
        self.fallback = None
        self.primary_conn = None

    # TODO: check if host is a DNS name / load balancer that can hold multiple addresses
    async def seed_node(self, cluster: Cluster, host: Host, peers: Peers) -> Optional[Node]:
        try:
            await self.validate_address(cluster, host)

            # TODO: set aliases when not set by load balancer detection logic

            node = Node(cluster, self)

            if await self.validate_peers(peers, node) == True:
                return node
        except Exception as e:
            # Log exception and continue to next alias
            logging.debug(f"Address {host.name} {host.port} failed: {e}")

            # TODO: exception logic
            exception = e

        if self.fallback != None:
            return None

        raise exception

    async def validate_node(self, cluster: Cluster, host: Host):
        # TODO: check if host has multiple addresses
        try:
            await self.validate_address(cluster, host)
            # TODO: set aliases
            return
        except Exception as e:
            logging.debug(f"Address {host.name} {host.port} failed: {e}")
            raise e

    async def validate_address(self, cluster: Cluster, host: Host):
        is_ip = host.is_ip()
        if not is_ip:
            raise AerospikeException("Invalid host passed to node validator")

        # TODO: also check for tls policy when creating connection
        conn = await Connection.new(host.name, host.port, cluster.conn_timeout)

        try:
            # TODO: check if cluster has authentication enabled

            commands = [
                "node",
                "partition-generation",
                "features",
            ]

            has_cluster_name = cluster.cluster_name != None and len(cluster.cluster_name) > 0
            if has_cluster_name:
                commands.append("cluster-name")

            # TODO: check for load balancer when determining service command
            # if host.is_loopback() is False:
            #     commands.append("service-clear-std")

            # Issue commands
            info_map = await Info.request(conn, commands)

            self.primary_host = host
            self.primary_conn = conn

            self._validate_node(info_map)
            self.validate_partition_generation(info_map)
            self.set_features(info_map)

            if has_cluster_name:
                self.validate_cluster_name(cluster, info_map)

            # TODO: address command logic
        except Exception as e:
            await self.primary_conn.close()
            raise e

    def _validate_node(self, info_map: dict[str, str]):
        if "node" not in info_map:
            raise InvalidNodeException(f"Node name is null")

        self.name = info_map["node"]

    def validate_partition_generation(self, info_map: dict[str, str]):
        gen_string = info_map.get("partition-generation")
        if gen_string == None:
            raise AerospikeException(f"Node {self.name} {self.primary_host} returned invalid partition-generation {gen_string}")

        try:
            gen = int(gen_string)
        except:
            raise AerospikeException(f"Node {self.name} {self.primary_host} returned invalid partition-generation {gen_string}")

        if gen == -1:
            raise AerospikeException(f"Node {self.name} {self.primary_host} is not yet fully initialized")

    def set_features(self, info_map: dict[str, str]):
        # TODO: actually set features in a class like the Java client
        try:
            features = info_map["features"]
            self.features = features.split(";")
        except:
            # Unexpected exception. Use defaults.
            pass

		# This client requires partition scan support. Partition scans were first
		# supported in server version 4.9. Do not allow any server node into the
		# cluster that is running server version < 4.9.
        if self.features and "pscans" not in self.features:
            raise AerospikeException(f"Node {self.name} {self.primary_host} version < 4.9. This client requires server version >= 4.9")

    def validate_cluster_name(self, cluster: Cluster, info_map: dict[str, str]):
        id = info_map.get("cluster_name")
        if id == None or cluster.cluster_name != id:
            raise AerospikeException(f"Node {self.name} {self.primary_host} expected cluster name '{cluster.cluster_name}' received '{id}'")

    async def validate_peers(self, peers: Peers, node: Node) -> bool:
        if peers is None:
            return True

        try:
            peers.refresh_count = 0
            await node.refresh_peers(peers)
        except Exception as e:
            await node.close()
            raise e

        if node.peers_count == 0:
			# Node is suspect because multiple seeds are used and node does not have any peers.
            if self.fallback == None:
                # TODO: leave off from here
                self.fallback = node
            else:
                await node.close()
            return False

        if self.fallback != None:
            logging.info("Skip orphan node")
            await self.fallback.close()
            self.fallback = None

        return True
