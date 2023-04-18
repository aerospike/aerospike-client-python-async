from aerospike_async.cluster import Cluster
from aerospike_async.host import Host

c = Cluster([Host("127.0.0.1", 3000)])
c.tend()
