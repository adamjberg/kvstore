import unittest
import threading
import time
from Request import *
from Response import *
from UDPClient import UDPClient
from NodeCircle import *

class TestKVStore(unittest.TestCase):
    PORT = 11111

    @classmethod
    def setUpClass(self):
        self.node_circle = NodeCircle()
        self.client = UDPClient(TestKVStore.PORT)
        self.client.socket.setblocking(False)
        self.client.socket.settimeout(0.5)

    def wait_for_pending_requests(self):
        while(len(self.client.pending_requests) > 0):
            pass

    def discover_down_nodes(self):
        request = PingRequest()
        for node in self.node_circle.nodes:
            self.client.send_request(request, node.get_addr(), None, self.discover_failed)
        self.wait_for_pending_requests()

    def discover_failed(self, request):
        print "Node down: " + str(request.dest_addr)
        self.node_circle.set_node_online_with_addr(request.dest_addr, False)

    def setUp(self):
        self.client_thread = threading.Thread(target=self.client.run)
        self.client_thread.start()
        self.discover_down_nodes()
        self.assertNotEqual(len(self.node_circle.get_online_nodes()), 0)
        self.test_node = self.node_circle.nodes[0]

    def tearDown(self):
        self.client_thread.join()

    def test_get_nonexistent_key(self):
        resp = self.get("test_get_nonexistent_key", self.test_node)
        self.assert_nonexistent_key(resp)

    def get(self, key, node):
        return self.send_request(GetRequest(key), node)

    def put(self, key, value, node):
        return self.send_request(PutRequest(key, value), node)

    def remove(self, key, node):
        return self.send_request(PutRequest(key, value), node)

    def send_request(self, request, node):
        self.response = None
        self.client.send_request(request, node.get_addr(), self.get_response)
        self.wait_for_pending_requests()
        return self.response

    def get_response(self, message):
        self.response = message

    def assert_no_response(self, response):
        self.assertTrue(response == None)

    def assert_response(self, response):
        self.assertTrue(response != None)

    def assert_nonexistent_key(self, response):
        self.assert_response_with_code(response, Response.NON_EXISTENT)

    def assert_response_with_code(self, response, expected_response_code):
        self.assert_response(response)
        self.assertEqual(response.payload[0], expected_response_code)

if __name__ == '__main__':
    unittest.main()