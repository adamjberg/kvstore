from threading import Thread
import random
import time
from Request import *

# Pings all nodes to update rtt stats and determine if node went down
class NodeMonitorThread(Thread):
    TIME_BETWEEN_RUNS = 5

    def __init__(self, sender, node_circle, received_data_queue):
        Thread.__init__(self)
        self.daemon = True
        self.sender = sender
        self.node_circle = node_circle
        self.received_data_queue = received_data_queue

    def run(self):
        if len(self.node_circle.nodes) == 0:
            return

        while(True):
            self.sender.send_request(PingRequest(), random.choice(self.node_circle.nodes))
            self.received_data_queue.put(None)

            time.sleep(NodeMonitorThread.TIME_BETWEEN_RUNS)