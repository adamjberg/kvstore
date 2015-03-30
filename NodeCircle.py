from __future__ import with_statement
import hashlib
import struct
from Node import Node

class NodeCircle:
    NUM_REPLICA_NODES = 2

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

    def get_node_with_addr(self, addr):
        for node in self.nodes:
            if node.get_addr() == addr:
                return node

    def set_node_online_with_addr(self, addr, online):
        node = self.get_node_with_addr(addr)
        node.online = online

    def is_my_node_responsible_for_key(self, key):
        nodes_for_key = self.get_nodes_for_key(key)
        return self.my_node in nodes_for_key

    def get_nodes_for_key(self, key):
        nodes = []
        master_node = self.get_master_node_for_key(key)
        nodes.append(master_node)

        for i in range(NodeCircle.NUM_REPLICA_NODES):
            successor = self.get_successor_for_node(nodes[i])
            if successor:
                nodes.append( successor )
            else:
                break

        return nodes

    def get_master_node_for_key(self, key):
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
        return self.get_predecessor_for_node(self.my_node)

    def get_predecessor_for_node(self, node):
        online_nodes = self.get_online_nodes()
        predecessor = online_nodes[online_nodes.index(node) - 1]
        if predecessor != node:
            return predecessor

        return None

    def get_successor(self):
        return self.get_successor_for_node(self.my_node)

    def get_successor_for_node(self, node):
        online_nodes = self.get_online_nodes()
        successor = online_nodes[online_nodes.index(node) - len(online_nodes) + 1]
        if successor != node:
            return successor

        return None

    def get_last_replica(self):
        return self.get_last_replica_for_node(self.my_node)

    def get_last_replica_for_node(self, node):
        successor = node
        for i in range(NodeCircle.NUM_REPLICA_NODES):
            successor = self.get_successor_for_node(successor)
            if successor == None:
                return None
        if successor != node:
            return successor
        return None

    def get_online_nodes(self):
        online_nodes = []
        for node in self.all_nodes:
            if node.online:
                online_nodes.append(node)
        return online_nodes
