from threading import Thread
import time
from Request import *

class MonitorNodeThread(Thread):
    MONITOR_DELAY_SECONDS = 5

    def __init__(self, client, node_circle, kvStore):
        Thread.__init__(self)
        self.daemon = True
        self.client = client
        self.node_circle = node_circle
        self.kvStore = kvStore

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
        down_node = self.node_circle.get_node_with_addr(request.dest_addr)
        print "MARK NODE DOWN " + str(down_node)

        request = SetOfflineRequest(down_node.get_addr())
        for node in self.node_circle.get_online_nodes():
            self.client.send_request(request, node.get_addr())

        last_replica_node = self.node_circle.get_last_replica()

        if last_replica_node:
            for key, value in self.kvStore.kv_dict.items():
                if self.node_circle.get_master_node_for_key(key) == down_node:
                    # For now ignore the response
                    self.client.send_request(InternalPutRequest(key,value), last_replica_node.get_addr())

        down_node.online = False


