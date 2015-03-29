from threading import Thread
import time
from Request import *

class MonitorNodeThread(Thread):
    MONITOR_DELAY_SECONDS = 5

    def __init__(self, client, node_circle):
        Thread.__init__(self)
        self.daemon = True
        self.client = client
        self.node_circle = node_circle

    def run(self):
        while True:
            self.ping_predecessor()
            time.sleep(MonitorNodeThread.MONITOR_DELAY_SECONDS)

    def ping_predecessor(self):
        predecessor = self.node_circle.get_predecessor()
        if predecessor:
            request = PingRequest()
            self.client.send_request(request, predecessor.get_addr(), None, self.ping_failed)

    def ping_failed(self, request):
        node = self.node_circle.get_node_with_addr(request.dest_addr)
        node.online = False
        request = SetOfflineRequest(node.get_addr())
        for node in self.node_circle.get_online_nodes():
            self.client.send_request(request, node.get_addr())
