import socket
import sys
import time
from UID import UID

class PendingRequest:
    def __init__(self, dest_node, uid, payload, onResponse, onFail):
        self.dest_node = dest_node
        self.uid = uid
        self.payload = payload
        self.onResponse = onResponse
        self.onFail = onFail
        self.attempts = 0
        self.timeout = Sender.DEFAULT_TIMEOUT
        self.last_attempt_time = time.time()
        self.first_attempt_time = time.time()

class CacheItem:
    def __init__(self, value):
        self.value = value
        self.timestamp = time.time()

class Sender:
    MAX_RETRY_ATTEMPTS = 4
    DEFAULT_TIMEOUT = 0.15
    CACHED_RESPONSE_EXPIRATION_TIME = 5
    MAX_RESPONSE_CACHE_SIZE = 1000

    def __init__(self, sock, node_circle):
        self.socket = sock
        self.node_circle = node_circle
        self.address = sock.getsockname()
        self.pending_requests = {}
        self.response_cache = {}

    def check_for_timeouts(self):
        cur_time = time.time()
        for uid, request in self.pending_requests.items():
            time_since_last_attempt = (cur_time - request.last_attempt_time)

            if time_since_last_attempt > request.timeout:
                if request.attempts >= Sender.MAX_RETRY_ATTEMPTS:
                    self.handle_failed_request(request)
                    continue

                request.attempts += 1
                request.timeout *= 2
                request.last_attempt_time = cur_time

                self.send_to_node(request.uid, request.payload, request.dest_node)

    def handle_successful_request(self, request):
        node = request.dest_node
        node.online = True
        node.update_rtt_stats(time.time() - request.first_attempt_time)

        onResponse = request.onResponse
        if onResponse is not None and hasattr(onResponse, '__call__'):
            onResponse()
        del self.pending_requests[str(request.uid)]

    def handle_failed_request(self, request):
        request.dest_node.online = False

        onFail = request.onFail
        if onFail is not None and hasattr(onFail, '__call__'):
            onFail()
        del self.pending_requests[str(request.uid)]

    def check_cached_responses(self, uid, sender_address):
        self.expire_cached_responses()
        try:
            cached_response = self.response_cache[str(uid)].value
            self.send_to(uid, cached_response, sender_address)
            return True
        except:
            return False

    def expire_cached_responses(self):
        cur_time = time.time()
        for uid, cache_item in self.response_cache.items():
            if cur_time - cache_item.timestamp > Sender.CACHED_RESPONSE_EXPIRATION_TIME:
                del self.response_cache[uid]

    def check_pending_requests(self, uid, sender_address):
        is_part_of_node_circle = self.node_circle.has_node_with_address(sender_address)

        if not is_part_of_node_circle:
            return False

        try:
            successful_request = self.pending_requests[str(uid)]
            self.handle_successful_request(successful_request)
            return True
        except:
            # If this UID is for the current node, ignore the response
            return self.is_uid_mine(uid)

    def is_uid_mine(self, uid):
        test_uid = UID(self.address)
        return uid.ip == test_uid.ip and uid.port == test_uid.port

    def get_time_til_next_timeout(self):
        time_til_next_timeout = sys.maxint
        for uid, request in self.pending_requests.items():
            if request.timeout < time_til_next_timeout:
                time_til_next_timeout = request.timeout
                
        return time_til_next_timeout

    def send_request(self, request, dest_node, onResponse = None, onFail = None):
        uid = UID(self.address)

        payload = request.get_bytes()
        pending_request = PendingRequest(dest_node, uid, payload, onResponse, onFail)
        self.pending_requests[str(uid)] = pending_request
        self.send_to_node(uid, payload, dest_node)

    def send_response(self, uid, response, dest_address):
        self.reply(uid, response.get_bytes(), dest_address)

    def reply(self, uid, payload, dest_address):
        self.add_to_response_cache(uid, payload)
        self.send_to(uid, payload, dest_address)

    def send_to(self, uid, payload, addr):
        self.socket.sendto(str(uid) + payload, addr)

    def send_to_node(self, uid, payload, node):
        self.send_to(uid, payload, node.get_addr())

    def add_to_response_cache(self, uid, data):
        if len(self.response_cache) < Sender.MAX_RESPONSE_CACHE_SIZE:
            self.response_cache[str(uid)] = CacheItem(data)