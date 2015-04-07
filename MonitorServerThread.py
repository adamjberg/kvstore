from threading import Thread
from datetime import datetime, timedelta
import simplejson as json
import socket
import subprocess
import time
import urllib2

def check_output(args):
    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE)
    out, err = p.communicate()
    return out

class MonitorServerThread(Thread):
    SERVER_ADDR = ("54.68.197.12", 41170)
    MONITOR_DELAY_SECONDS = 10

    def __init__(self, node_circle, kvStore):
        Thread.__init__(self)
        self.daemon = True
        self.node_circle = node_circle
        self.kvStore = kvStore
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(("", 0))
        self.hostname = socket.gethostname()
        try:
            self.loc = urllib2.urlopen("http://ip-api.com/json/", timeout=2).read()
        except:
            self.loc = {"status" :"fail"}

        self.service_start_time = datetime.now()

    def run(self):
        while True:
            try:
                self.socket.sendto(self.get_data(), MonitorServerThread.SERVER_ADDR)
            except:
                pass
            time.sleep(MonitorServerThread.MONITOR_DELAY_SECONDS)

    def get_data(self):
        data = {}
        data["hostname"] = self.hostname
        data["systemUptime"] = ""
        data["spaceAvailable"] = self.get_available_space()
        data["averageLoads"] = self.get_average_load()
        data["serviceUptime"] = self.get_service_uptime()
        data["loc"] = self.loc
        data["logs"] = {}
        data["kvstore"] = self.get_successors_data()
        data["index"] = self.node_circle.my_node.location

        return json.JSONEncoder().encode(data)

    def get_system_uptime(self):
        system_start_time = datetime.strptime(check_output(["uptime", "-s"]), "%Y-%m-%d %H:%M:%S\n")
        return str(datetime.now() - system_start_time)

    def get_service_uptime(self):
        return str(datetime.now() - self.service_start_time)

    def get_average_load(self):
        uptime_string = check_output(["uptime"])
        return uptime_string[-17:]

    def get_available_space(self):
        df_string = check_output(["df", "-h"])
        df_array = df_string.split("\n")
        for line in df_array:
            if line[-1] == "/":
                return line[28:32]

    def get_successors_data(self):
        data = []
        for node in self.node_circle.all_nodes:
            node_data = {}
            node_data["hostname"] = node.hostname
            node_data["port"] = node.port
            node_data["location"] = node.location
            node_data["status"] = node.online
            node_data["lastUpdateDate"] = ""
            node_data["spaceAvailable"] = 0
            node_data["kvstore"] = {}

            if node == self.node_circle.my_node:
                node_data["kvstore"] = self.kvStore.kv_dict
                node_data["spaceAvailable"] = self.kvStore.space_available

            data.append(node_data)

        return data
