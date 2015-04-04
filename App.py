import socket
import sys
from KVStore import KVStore
from Request import *
from UDPClient import UDPClient
from RequestHandler import *
from NodeCircle import *

class App:

    def __init__(self):
        self.node_circle = NodeCircle()

        if self.init_client() is False:
            print "Failed to bind to a port."
            sys.exit()

        self.kvStore = KVStore()
        self.request_handler = RequestHandler(self.client, self.kvStore, self.node_circle)
        
        self.send_join_request()

        self.client.run()

    def init_client(self):
        nodes_for_my_ip = []

        for node in self.node_circle.nodes:
            ip = node.ip
            if self.does_ip_match_mine(ip):
                nodes_for_my_ip.append(node)

        for node in nodes_for_my_ip:
            try:
                self.client = UDPClient(node.get_addr(), self.handle_message)
                self.node_circle.my_node = node
                print "Connected on port: " + str(node.get_addr())
                return True
            except socket.error:
                pass

        return False

    def does_ip_match_mine(self, ip):
        if ip.startswith("127.") or ip.startswith("localhost"):
            return True
        elif ip == socket.gethostbyname(socket.gethostname()):
            return True
        elif ip == socket.gethostname():
            return True
        return False  

    def send_join_request(self):
        request = JoinRequest()
        successor = self.node_circle.get_successor()
        if successor:
            self.client.send_request(request, successor.get_addr(), None, self.join_request_failed)
        else:
            print "First 1 Starting New Service"
            monitor_node_thread = MonitorNodeThread(self.client, self.node_circle, self.kvStore)
            monitor_node_thread.start()
            self.request_handler.send_set_online_request()

    def join_request_failed(self, request):
        self.node_circle.set_node_online_with_addr(request.dest_addr, False)
        self.send_join_request()

    def handle_message(self, message):
        self.request_handler.handle_message(message)

if __name__ == "__main__":
    try:
        App()
    except KeyboardInterrupt:
        sys.exit()
