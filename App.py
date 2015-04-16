import os
import socket
import sys
import Queue
from KVStore import KVStore
from Request import *
from RequestHandler import *
from NodeCircle import *
from ReceiverThread import *

class App:

    def __init__(self):
        self.init_node_circle()
        self.init_receiver_thread()
        self.main_loop()

    def init_node_circle(self):
        if len(sys.argv) > 1:
            filename = sys.argv[1]
        else:
            filename = "hosts.txt"

        nodes = []
        with open(filename) as f:
            lines = [x.strip('\n') for x in f.readlines()]
        for line in lines:
            host, port, location = line.split(":")
            node = Node(host, port, location)
            nodes.append(node)

        self.socket, my_node = self.get_my_socket_and_node(nodes)

        self.node_circle = NodeCircle(nodes, my_node)

    def get_my_socket_and_node(self, nodes):
        nodes_for_my_ip = []

        for node in nodes:
            ip = node.ip
            if self.does_ip_match_mine(ip):
                nodes_for_my_ip.append(node)

        for node in nodes_for_my_ip:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.bind(node.get_addr())
                sock.setblocking(True)

                print "Connected to " + str(node)

                return (sock, node)
            except socket.error:
                pass

        print "Could not find open port, Exiting"
        sys.exit()

    def does_ip_match_mine(self, ip):
        return ip.startswith("127.") or ip.startswith("localhost") or ip == socket.gethostbyname(socket.gethostname()) or ip == socket.gethostname()

    def init_receiver_thread(self):
        self.received_data_queue = Queue.Queue()
        self.receiver_thread = ReceiverThread(self.socket, self.received_data_queue)
        self.receiver_thread.start()

    def main_loop(self):
        while True:
            data, sender_address = self.wait_until_next_event_or_data()
            request = Request.from_bytes(data)
            if request:
                print "DATA"
                self.received_data_queue.task_done()

    def wait_until_next_event_or_data(self):
        try:
            return self.received_data_queue.get(timeout=1000)
        except Queue.Empty:
            return (None, None)

if __name__ == "__main__":
    App()
