from __future__ import with_statement
import hashlib
import struct
import sys
from Node import Node

class NodeCircle:
    NUM_REPLICA_NODES = 1

    def __init__(self, nodes, my_node):
        self.nodes = nodes
        self.my_node = my_node

    def is_my_key(self, key):
        return self.my_node in self.get_nodes_for_key(key)

    def get_location_for_key(self, key):
        return struct.unpack('B', hashlib.sha256(key).digest()[0])[0]

    def get_nodes_for_key(self, key):
        nodes = []
        online_nodes = self.get_online_nodes()
        master_node = None
        location = self.get_location_for_key(key)
        for node in online_nodes:
            if master_node is None:
                master_node = node

            if node.location >= location:
                master_node = node
                break

        nodes.append(master_node)
        successor = master_node
        for i in range(NodeCircle.NUM_REPLICA_NODES):
            successor = self.get_successor_for_node(successor)
            if successor:
                nodes.append(successor)
            else:
                break

        return nodes

    def get_successor_for_node(self, node):
        online_nodes = self.get_online_nodes()

        try:
            successor = self.online_nodes[online_nodes.index(node) - len(online_nodes) + 1]
            if successor != node:
                return successor
        except:
            return None

    def get_optimal_node_for_key(self, key):
        nodes_for_key = self.get_nodes_for_key(key)

        for node in nodes_for_key:
            return node

    def get_online_nodes(self):
        return [node for node in self.nodes if node.online]