class NodeValidator:
    async def seed_node(self, cluster: Cluster, seed: Host):
        conn = await Connection.new(seed.name, seed.port)
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
