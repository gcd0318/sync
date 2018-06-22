from config import period, host_ips
from node import Node

class Cluster(object):
    def __init__(self, node_ips=host_ips, timing_s=period):
        self.nodes = []
        for node_ip in node_ips:
            self.nodes.append(Node(node_ip, hosts=node_ips, rootpath='/home/guochen/sync'))
        self.timing = timing_s


if ('__main__' == __name__):
    c = Cluster()
    for n in c.nodes:
        print(n.get_free_size())
