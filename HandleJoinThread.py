from threading import Thread
import time
from Request import *

class HandleJoinThread(Thread):
    def __init__(self, client, node_circle, node, kvStore):
        Thread.__init__(self)
        self.client = client
        self.node_circle = node_circle
        self.node = node
        self.kvStore = kvStore

    def run(self):
        # temporarily set the node online so that get_master_node_for_key will be correct
        self.node.online = True
        requests = []

        # TODO: This will send replicas to the joining node that it doesn't need
        for key, value in self.kvStore.kv_dict.items():
            requests.append(InternalPutRequest(key, value))
        self.node.online = False
        
        if len(requests) > 0:
            self.num_pending_requests = len(requests)
            for request in requests:
                self.client.send_request(request, self.node.get_addr(), self.put_success)
        else:
            self.send_successful_join()

    def put_success(self, message):
        self.num_pending_requests -= 1
        if self.num_pending_requests == 0:
            self.send_successful_join()

    def send_successful_join(self):
        successful_join_request = JoinSuccessRequest()
        self.client.send_request(successful_join_request, self.node.get_addr(), self.join_ack_received, self.join_ack_failed)

    def join_ack_received(self, message):
        self.node.online = True
        self.purge_not_needed_keys()

    def join_ack_failed(self, request):
        pass

    def purge_not_needed_keys(self):
        for key, value in self.kvStore.kv_dict.items():
            if self.node_circle.is_my_node_responsible_for_key(key):
                continue
            self.kvStore.remove(key)