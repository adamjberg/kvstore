from threading import Thread
import time
from NodeCircle import *
from Request import *

class DataMigrationThread(Thread):
    TIME_BETWEEN_MIGRATIONS = 1

    def __init__(self, sender, node_circle, kv_store, received_data_queue):
        Thread.__init__(self)
        self.daemon = True
        self.sender = sender
        self.node_circle = node_circle
        self.kv_store = kv_store
        self.received_data_queue = received_data_queue
        self.offline_nodes_last_run = []

    def run(self):
        if len(self.node_circle.nodes) == 0:
            return

        while(True):
            offline_nodes = []
            for node in self.node_circle.nodes:
                if self.did_node_just_go_offline(node):
                    self.migrate_data_for_node(node)

                if node.online == False:
                    offline_nodes.append(node)

            self.offline_nodes_last_run = offline_nodes
            time.sleep(DataMigrationThread.TIME_BETWEEN_MIGRATIONS)

    def did_node_just_go_offline(self, node):
        if node.online or node in self.offline_nodes_last_run or node == self.node_circle.my_node:
            return False

        return True

    def migrate_data_for_node(self, node):
        locations_lost = self.node_circle.get_all_locations_for_node(node)

        my_locations = self.node_circle.get_locations_for_my_node()

        for location in locations_lost:
            if location in my_locations:
                self.migrate_data_at_location(location)

    # Sends data from a certain location to 
    def migrate_data_at_location(self, location):
        dict_to_send = self.kv_store.kv_dict[location]

        replica_nodes = self.node_circle.get_replica_nodes_for_my_node()

        if len(replica_nodes) < NodeCircle.NUM_REPLICA_NODES:
            return

        dest_node = replica_nodes[-1]

        for key, value in dict_to_send.iteritems():
            self.sender.send_request(PutRequest(key, value), dest_node)

        self.received_data_queue.put((None, None))