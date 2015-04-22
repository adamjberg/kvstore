from threading import Thread
import time
from Request import *

class HandleJoinThread(Thread):
    def __init__(self, sender, node_circle, node, kv_store):
        Thread.__init__(self)
        self.daemon = True
        self.sender = sender
        self.node_circle = node_circle
        self.node = node
        self.kv_store = kv_store

    def run(self):
        locations_for_new_node = self.node_circle.get_all_locations_for_node(self.node)

        for location in locations_for_new_node:
            dict_to_send = self.kv_store.kv_dict[location]

            for key, value in dict_to_send.iteritems():
                self.sender.send_request(InternalPutRequest(key, value), self.node)
                time.sleep(0.01)