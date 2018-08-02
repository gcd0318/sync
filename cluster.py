from config import period, host_ips
from node import Node

class Cluster(object):
    def __init__(self, node_ips=host_ips, timing_s=period):
        self.nodes = []
        for node_ip in node_ips:
            self.nodes.append(Node(node_ip, hosts=node_ips, rootpath='/home/guochen/sync'))
        self.timing = timing_s

    def get_status(self):
        res = {}
        for node in self.nodes:
            res[node.ip] = node.get_status()
        return res

    def transfer_from_incoming(self):
        for node in self.nodes:
            for filename in node.
            node.transfor_file_to_remote()

    def sync_all_db(self):
        for node in self.nodes:
            pass


if ('__main__' == __name__):
    c = Cluster()
    print(c.get_status())
    for n in c.nodes:
        print(n.get_status())
