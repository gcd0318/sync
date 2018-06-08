from config import period, host_ips

class Cluster(object):
    def __init__(self, node_ips=host_ips, timing_s=period):
        self.node_ips = []
        for node_ip in node_ips:
            self.node_ips.append(node_ip)
        self.timing = timing_s
