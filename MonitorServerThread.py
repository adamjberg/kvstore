from threading import Thread
import simplejson as json
import socket
import time
import urllib2

class MonitorServerThread(Thread):
    SERVER_ADDR = ("54.68.197.12", 41170)
    MONITOR_DELAY_SECONDS = 10

    def __init__(self, node_circle, kvStore):
        Thread.__init__(self)
        self.daemon = True
        self.node_circle = node_circle
        self.kvStore = kvStore
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(("", 41171))
        self.hostname = socket.gethostname()
        self.loc = urllib2.urlopen("http://ip-api.com/json/").read()

    def run(self):
        while True:
            self.socket.sendto(self.get_data(), MonitorServerThread.SERVER_ADDR)
            time.sleep(MonitorServerThread.MONITOR_DELAY_SECONDS)

    def get_data(self):
        data = {}
        data["hostname"] = self.hostname
        data["systemUptime"] = 0
        data["spaceAvailable"] = 300
        data["averageLoads"] = ""
        data["serviceUptime"] = ""
        data["loc"] = self.loc
        data["logs"] = {}
        data["kvstore"] = self.get_successors_data()
        data["index"] = self.node_circle.my_node.location

        return json.JSONEncoder().encode(data)

    def get_successors_data(self):
        data = []
        for node in self.node_circle.all_nodes:
            node_data = {}
            node_data["hostname"] = node.hostname
            node_data["port"] = node.port
            node_data["location"] = node.location
            node_data["status"] = node.online
            node_data["lastUpdateDate"] = "NOW"
            node_data["spaceAvailable"] = 0
            node_data["kvstore"] = {}

            if node == self.node_circle.my_node:
                node_data["kvstore"] = self.kvStore.kv_dict
                node_data["spaceAvailable"] = self.kvStore.space_available

            data.append(node_data)

        return data
