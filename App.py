from __future__ import with_statement
import hashlib
import socket
import sys
import time
from KVStore import KVStore
from Message import Message
from Node import Node
from Response import *
from Request import *
from UDPClient import UDPClient
from UID import UID
from RequestHandler import *

class App:

    def __init__(self):
        self.nodes = []
        self.init_nodes_from_file()

        if self.init_client() is False:
            print "Failed to bind to a port."
            sys.exit()

        self.kvStore = KVStore()
        self.request_handler = RequestHandler(self.client, self.kvStore, self.nodes, self.my_node)
        self.send_set_online_request()
        self.client.run()

    def init_nodes_from_file(self):
        with open("hosts.txt") as f:
            lines = [x.strip('\n') for x in f.readlines()]
        for line in lines:
            host, port, location = line.split(":")
            self.nodes.append(Node(host, port, location))

    def init_client(self):
        nodes_for_my_ip = []

        for node in self.nodes:
            ip = node.ip
            if self.does_ip_match_mine(ip):
                nodes_for_my_ip.append(node)

        for node in nodes_for_my_ip:
            try:
                self.client = UDPClient(node.port, self.handle_message)
                self.my_node = node
                print "Connected on port: " + str(node.port)
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

    def send_set_online_request(self):
        request = SetOnlineRequest()
        for node in self.nodes:
            if node != self.my_node:
                self.client.send_request(request, node.get_addr(), self.set_online_success, self.set_online_failed)

    def set_online_success(self, message):
        self.set_node_online_with_addr(message.sender_addr, True)

    def set_online_failed(self, request):
        self.set_node_online_with_addr(request.dest_addr, False)

    def set_node_online_with_addr(self, addr, online):
        for node in self.nodes:
            if node.get_addr() == addr:
                node.online = online
                break

    def handle_message(self, message):
        self.request_handler.handle_message(message)

if __name__ == "__main__":
    App()