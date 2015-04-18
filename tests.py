import unittest
import random
import socket
import threading
import time
from Request import *
from Response import *
from Node import *

node_circle = None

class TestKVStore(unittest.TestCase):
    PORT = 11111

    def discover_down_nodes(self):
        global node_circle

        request = PingRequest()
        for node in node_circle.get_online_nodes():
            resp = self.send_request(request, node)
            if resp == None:
                print "Node down: " + str(node)
                node.online = False

    def setUp(self):
        global node_circle
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(("", 0))
        self.socket.setblocking(True)
        self.socket.settimeout(0.5)
        self.last_uid_time = time.time()
        self.used_uids = []
        self.num_requests = 0;
        self.failed_requests = 0
        self.successful_requests = 0
        self.incorrect_responses = 0
        self.missing_responses = 0

        self.socket.settimeout(5)
        self.test_node = Node(socket.gethostname(), 33333, 0)

    def tearDown(self):
        self.socket.close()

        if self.missing_responses > 0:
            print "missing_responses " + str(self.missing_responses)
        if self.failed_requests > 0:
            print "failed_requests " + str(self.failed_requests)
        if self.incorrect_responses > 0:
            print "incorrect_responses " + str(self.incorrect_responses)

        self.assertEqual(self.missing_responses, 0)
        self.assertEqual(self.failed_requests, 0)
        self.assertEqual(self.incorrect_responses, 0)

        pass

    def test_unrecognized_command(self):
        req = Request(chr(100))
        self.send_request(req)

    def test_get_nonexistent_key(self):
        resp = self.get("test_get_nonexistent_key")
        self.assert_nonexistent_key(resp)

    def test_put_and_remove(self):
        key = "test_put"
        value = "test_put"
        self.assert_successful_request(self.put(key, value))
        self.assert_get_value(self.get(key), value)
        self.assert_successful_request(self.remove(key))

    def test_cached_response(self):
        key = "test_cached_response"
        uid = self.get_uid()

        self.assert_nonexistent_key(self.get(key, None, uid))
        # check for same response
        self.assert_nonexistent_key(self.put(key, "", None, uid))
        # Make sure the put didn't happen
        self.assert_nonexistent_key(self.get(key))

    def test_replace(self):
        key = "test_replace"
        val1 = "test_replace1"
        val2 = "test_replace2"
        self.put(key, val1)
        self.put(key, val2)
        self.assert_get_value(self.get(key), val2)

    def test_remove_nonexistent(self):
        self.assert_nonexistent_key(self.remove("test_remove_nonexistent"))

    def test_concurrent_put(self):
        uid1 = self.get_uid()
        uid2 = self.get_uid()
        val1 = "val1"
        val2 = "val2"
        uid2.timestamp += 1

        key = "test_concurrent_put"
        self.put(key, val1, None, uid1)
        self.put(key, val2, None, uid2)
        self.put(key, val1, None, uid1)

        self.assert_get_value(self.get(key), val2)

    # def test_put_one_get_all(self):
    #     key = "test_put_one_get_all"
    #     val = "test_put_one_get_all"
    #     self.put(key, val)
    #     for node in self.online_nodes:
    #         self.assert_get_value(self.get(key, node), val)
    #     self.remove(key)

    def test_many_put(self):
        base_key = "test_many_put"
        base_val = "test_many_put"

        for i in range(100):
            key = base_key + str(i)
            val = base_val + str(i)
            self.assert_successful_request(self.put(key, val))

        for i in range(100):
            key = base_key + str(i)
            val = base_val + str(i)
            self.assert_get_value(self.get(key), val)
            self.assert_successful_request(self.remove(key))

    # def test_shutdown(self):
    #     self.assert_successful_request(self.shutdown())
    #     self.assert_no_response(self.get(""))

    def get_uid(self):
        uid = None

        if time.time() - self.last_uid_time > 0.001:
            self.used_uids = []

        while uid is None or uid in self.used_uids:
            uid = UID(("127.0.0.1", 0))

        self.last_uid_time = time.time()
        self.used_uids.append(uid)
        return uid

    def get(self, key, node = None, uid = None):
        return self.send_request(GetRequest(key), node, uid)

    def put(self, key, value, node = None, uid = None):
        return self.send_request(PutRequest(key, value), node, uid)

    def remove(self, key, node = None, uid = None):
        return self.send_request(RemoveRequest(key), node, uid)

    def shutdown(self, node = None):
        return self.send_request(ShutdownRequest(), node)

    def send_request(self, request, node = None, uid = None):
        if node is None:
            node = self.test_node

        if uid is None:
            uid = UID(("127.0.0.1", 0))

        self.num_requests += 1
        self.socket.sendto(uid.get_bytes() + request.get_bytes(), node.get_addr())
        try:
            data, addr = self.socket.recvfrom(20000)
            self.assertEqual(uid.get_bytes(), data[:16])
            return data[16:]
        except:
            return None

    def get_response(self, message):
        self.response = message

    def assert_no_response(self, response):
        if response is not None:
            self.incorrect_responses += 1
        self.assertTrue(response == None)

    def assert_response(self, response):
        if response is None:
            self.missing_responses += 1

    def assert_nonexistent_key(self, response):
        self.assert_response_with_code(response, Response.NON_EXISTENT)

    def assert_get_value(self, response, expected_value):
        self.assert_successful_request(response)

        if response is None:
            return

        if response[3:] != expected_value:
            self.incorrect_responses += 1

    def assert_successful_request(self, response):
        self.assert_response(response)
        if response is None:
            return
        if response[0] != Response.SUCCESS:
            self.failed_requests += 1
        else:
            self.successful_requests += 1

    def assert_response_with_code(self, response, expected_response_code):
        self.assert_response(response)
        if response is None:
            return

        if response[0] != expected_response_code:
            self.incorrect_responses += 1

if __name__ == '__main__':
    unittest.main()