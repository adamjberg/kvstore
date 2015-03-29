from __future__ import with_statement
import hashlib
import struct
from Node import Node

class NodeCircle:
    def __init__(self):
        self.all_nodes = []
        self.nodes = []
        self.init_nodes_from_file()

    def init_nodes_from_file(self):
        with open("hosts.txt") as f:
            lines = [x.strip('\n') for x in f.readlines()]
        for line in lines:
            host, port, location = line.split(":")
            node = Node(host, port, location)
            self.all_nodes.append(node)
            self.nodes.append(node)

    def set_my_node(self, my_node):
        self.my_node = my_node
        self.nodes.remove(my_node)

    def set_node_online_with_addr(self, addr, online):
        for node in self.nodes:
            if node.get_addr() == addr:
                node.online = online
                break

    def get_responsible_node_for_key(self, key):
        dest_node = None
        location = self.get_location_for_key(key)
        for node in self.all_nodes:
            if node.online == False:
                continue

            if dest_node is None or node.location >= location:
                dest_node = node

        return dest_node

    def get_location_for_key(self, key):
        return struct.unpack('B', hashlib.sha256(key).digest()[0])[0]

    def get_predecessor(self):
        for node in self.all_nodes:
            predecessor = node
