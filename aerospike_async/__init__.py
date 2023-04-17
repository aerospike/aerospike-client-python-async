from .host import Host
from .cluster import Cluster

class AsyncClient:
    def __init__(self, hosts: list[Host]):
        self.cluster = Cluster(seeds=hosts)
