from __future__ import annotations
from dataclasses import dataclass

from .host import Host
from .connection import Connection
from .info import Info
from .exceptions import InvalidNodeException, AerospikeException

# TODO
class Node:
    def __init__(self, cluster: Cluster, nv: NodeValidator):
        self.cluster = cluster
        self.pool = []
        self.address = nv.primary_address

    def create_min_connections(self):
        for _ in range(self.cluster.min_conns_per_node):
            conn = Connection(self.address[0], self.address[1])
            self.pool.append(conn)

@dataclass
class NodeFeatures:
    has_partition_scan: bool = False
    has_query_show: bool = False
    has_batch_any: bool = False
    has_partition_query: bool = False

class NodeValidator:
    def seed_node(self, cluster: Cluster, seed: Host):
        conn = Connection(seed.name, seed.port)
        commands = [
            "node",
            "partition-generation",
            "features"
        ]
        responses = Info.request(conn, commands)

        if "node" not in responses:
            raise InvalidNodeException("Node name is null")

        self.name = responses["node"]
        self.primary_host = seed
        self.primary_address = (seed.name, seed.port)

        try:
            gen_str = responses.get("partition-generation")
            gen = int(gen_str)
        except:
            # Either generation doesn't exist or it is an invalid value
            name = responses["name"]
            error_msg = f"Node {name} {seed} returned invalid partition generation {gen}"
            raise InvalidNodeException(error_msg)

        if gen == -1:
            error_msg = f"Node {name} {seed} is not yet fully initialized"
            raise InvalidNodeException(error_msg)

        self.set_features(responses)

        return Node(cluster, self)

    def set_features(self, responses: dict[str, str]):
        # TODO: What happens if features doesn't exist in node?
        features = responses.get("features")
        features = features.split(";")
        # TODO: node features must be assigned to node
        node_fts = NodeFeatures()
        if "pscans" in features:
            node_fts.has_partition_scan = True
        if "query-show" in features:
            node_fts.has_query_show = True
        if "batch-any" in features:
            node_fts.has_batch_any = True
        if "pquery" in features:
            node_fts.has_partition_query = True

        if node_fts.has_partition_scan is False:
            # TODO: assuming this is a client exception?
            raise AerospikeException("Node {self.name} {self.primary_host} version < 4.9. " \
                                     "This client requires server version >= 4.9")

class Cluster:
    def __init__(self, seeds: list[Host]):
        self.seeds = seeds
        self.nodes = []
        self.min_conns_per_node = 10

    def tend(self):
        if len(self.nodes) == 0:
            self.seed_nodes()

    def seed_nodes(self):
        # TODO: does it make more sense to make the node validator a class obj?
        nv = NodeValidator()
        for seed in self.seeds:
            node = nv.seed_node(self, seed)
            node.create_min_connections()
