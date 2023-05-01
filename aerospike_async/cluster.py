# from __future__ import annotations
# from dataclasses import dataclass

# from .host import Host
# from .connection import Connection
# from .info import Info
# from .exceptions import InvalidNodeException, AerospikeException
# # from .node_validator import NodeValidator

# # TODO

# @dataclass
# class NodeFeatures:
#     has_partition_scan: bool = False
#     has_query_show: bool = False
#     has_batch_any: bool = False
#     has_partition_query: bool = False

# class Cluster:
#     def __init__(self, seeds: list[Host]):
#         self.seeds = seeds
#         self.nodes = []
#         self.min_conns_per_node = 10

#     def tend(self):
#         if len(self.nodes) == 0:
#             self.seed_nodes()

#     def seed_nodes(self):
#         # TODO: does it make more sense to make the node validator a class obj?
#         nv = NodeValidator()
#         for seed in self.seeds:
#             node = nv.seed_node(self, seed)
#             node.create_min_connections()
