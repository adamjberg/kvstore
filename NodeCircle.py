from __future__ import with_statement
import hashlib
import struct
import sys
from Node import Node

class NodeCircle:
    def __init__(self, nodes, my_node):
        self.nodes = nodes
        self.my_node = my_node

    def is_my_key(self, key):
        return True

    def get_location_for_key(self, key):
        return struct.unpack('B', hashlib.sha256(key).digest()[0])[0]