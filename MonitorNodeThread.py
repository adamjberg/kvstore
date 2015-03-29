from threading import Thread
import time

class MonitorNodeThread(Thread):
    def __init__(self, client, node_circle):
        Thread.__init__(self)
        self.daemon = True
        self.client = client
        self.node_circle = node_circle

    def run(self):
        while True:
            time.sleep(5)