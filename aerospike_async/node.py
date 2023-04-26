class Node:
    def __init__(self, cluster: Cluster, nv: NodeValidator):
        self.cluster = cluster
        self.pool = []
        self.address = nv.primary_address

    def create_min_connections(self):
        for _ in range(self.cluster.min_conns_per_node):
            conn = Connection(self.address[0], self.address[1])
            self.pool.append(conn)
