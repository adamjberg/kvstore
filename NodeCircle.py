from __future__ import with_statement
import hashlib
import struct
import sys
from Node import Node

class NodeCircle:
    NUM_REPLICA_NODES = 2
    CIRCLE_SIZE = 256

    def __init__(self, nodes, my_node):
        self.all_nodes = list(nodes)
        self.nodes = nodes
        self.nodes.remove(my_node)
        self.my_node = my_node

    def has_node_with_address(self, address):
        for node in self.all_nodes:
            if node.get_addr() == address:
                return True

        return False

    def is_my_key(self, key):
        return self.my_node in self.get_nodes_for_key(key)

    def get_location_for_key(self, key):
        return struct.unpack('B', hashlib.sha256(key).digest()[0])[0]

    def get_all_locations_for_node(self, node):
        predecessors = [node]
        for i in range(NodeCircle.NUM_REPLICA_NODES):
            node = self.get_predecessor_for_node(node)
            if node:
                predecessors.append(node)
            else:
                break

        locations = []

        for node in predecessors:
            locations += self.get_locations_for_node(node)

        return locations

    def get_locations_for_my_node(self):
        return self.get_locations_for_node(self.my_node)

    def get_locations_for_node(self, node):
        predecessor = self.get_predecessor_for_node(node)

        if predecessor is None:
            return range(NodeCircle.CIRCLE_SIZE)

        locations = range(predecessor.location + 1, node.location + 1)
        if len(locations) > 0:
            return locations
        else:
            return range(node.location + 1) + range(predecessor.location + 1, NodeCircle.CIRCLE_SIZE)

    def get_replica_nodes_for_my_node(self):
        return self.get_replica_nodes_for_node(self.my_node)

    def get_replica_nodes_for_node(self, node):
        replica_nodes = []
        successor = node
        for i in range(NodeCircle.NUM_REPLICA_NODES):
            successor = self.get_successor_for_node(successor)
            if successor and successor != node:
                replica_nodes.append(successor)
            else:
                break

        return replica_nodes

    def get_replica_nodes_for_key(self, key):
        nodes = self.get_nodes_for_key(key)
        try:
            nodes.remove(self.my_node)
        except:
            pass
        return nodes

    def get_nodes_for_key(self, key):
        location = self.get_location_for_key(key)
        return self.get_nodes_for_location(location)

    def get_nodes_for_location(self, location):
        nodes = []
        online_nodes = self.get_online_nodes()
        master_node = None
        for node in online_nodes:
            if master_node is None:
                master_node = node

            if node.location >= location:
                master_node = node
                break

        nodes.append(master_node)
        nodes += self.get_replica_nodes_for_node(master_node)
        return nodes

    def get_predecessor_for_node(self, node):
        if node.online:
            online_nodes = self.get_online_nodes()
        else:
            node.online = True
            online_nodes = self.get_online_nodes()
            node.online = False

        predecessor = online_nodes[online_nodes.index(node) - 1]
        if predecessor != node:
            return predecessor

        return None

    def get_successor_for_node(self, node):
        if node.online:
            online_nodes = self.get_online_nodes()
        else:
            node.online = True
            online_nodes = self.get_online_nodes()
            node.online = False

        successor = online_nodes[online_nodes.index(node) - len(online_nodes) + 1]
        if successor != node:
            return successor

        return None

    def get_optimal_node_for_key(self, key):
        nodes_for_key = self.get_nodes_for_key(key)

        min_avg_rtt = None
        optimal_node = None
        
        for node in nodes_for_key:
            if min_avg_rtt is None:
                min_avg_rtt = node.average_rtt
                optimal_node = node
            elif node.average_rtt < min_avg_rtt:
                min_avg_rtt = node.average_rtt
                optimal_node = node

        return optimal_node

    def get_online_nodes(self):
        return [node for node in self.all_nodes if node.online]