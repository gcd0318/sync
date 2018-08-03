from config import period, host_ips
from node import Node
import xmlrpc.client
from config import rpc_port

class Cluster(object):
    def __init__(self, node_ips=host_ips, timing_s=period):
        self.node_ips = node_ips
        self.servers = {}
        for node_ip in self.node_ips:
            self.servers[node_ip] = xmlrpc.client.ServerProxy('http://' + node_ip + ':' + str(rpc_port))
#        for node_ip in node_ips:
#            self.nodes.append(Node(node_ip, hosts=node_ips, rootpath='/home/guochen/sync'))
        self.timing = timing_s

    def get_status(self):
        res = {}
        for node_ip in self.node_ips:
            res[node_ip] = self.servers[node_ip].get_status()
        return res

    def transfer_from_incoming(self):
        for node in self.nodes:
            for filename in node:
                pass
                node.transfor_file_to_remote()

    def sync_all_db(self):
        for node in self.nodes:
            pass


if ('__main__' == __name__):
    c = Cluster()
    for nip in c.node_ips:
        print(nip, Node(nip).get_status())
    print(c.get_status())
