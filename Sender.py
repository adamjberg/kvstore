import socket
import sys
import time
from UID import UID

class PendingRequest:
    def __init__(self, dest_addr, uid, payload, onResponse, onFail):
        self.dest_addr = dest_addr
        self.uid = uid
        self.payload = payload
        self.onResponse = onResponse
        self.onFail = onFail
        self.attempts = 0
        self.timeout = Sender.DEFAULT_TIMEOUT_IN_MS
        self.last_attempt_time = time.time()

class CacheItem:
    def __init__(self, value):
        self.value = value
        self.timestamp = time.time()

class Sender:
    MAX_RETRY_ATTEMPTS = 3
    DEFAULT_TIMEOUT_IN_MS = 100
    CACHED_RESPONSE_EXPIRATION_TIME = 5
    MAX_RESPONSE_CACHE_SIZE = 1000

    def __init__(self, sock):
        self.socket = sock
        self.address = sock.getsockname()
        self.pending_requests = {}
        self.response_cache = {}

    def check_for_timeouts(self):
        cur_time = time.time()
        for uid, request in self.pending_requests.items():
            time_since_last_attempt = (cur_time - request.last_attempt_time) * 1000

            if time_since_last_attempt > request.timeout:
                if request.attempts >= Sender.MAX_RETRY_ATTEMPTS:
                    self.fail_request(request)
                    continue

                request.attempts += 1
                request.timeout *= 2
                request.last_attempt_time = cur_time

                self.send_to(request.uid, request.payload, request.dest_addr)

    def fail_request(self, request):
        uid_bytes = str(request.uid)

        if request.onFail is not None and hasattr(request.onFail, '__call__'):
            request.onFail(request)

        del self.pending_requests[uid_bytes]

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

    def check_pending_requests(self, uid):
        try:
            successful_request = self.pending_requests[str(uid)]
            onResponse = successful_request.onResponse
            if onResponse is not None and hasattr(onResponse, '__call__'):
                onResponse(message)
            del self.pending_requests[str(uid)]
            return True
        except:
            return False

    def get_time_til_next_timeout(self):
        time_til_next_timeout = sys.maxint
        for uid, request in self.pending_requests.items():
            if request.timeout < time_til_next_timeout:
                time_til_next_timeout = request.timeout
                
        return time_til_next_timeout

    def send_request(self, request, dest_addr, onResponse = None, onFail = None):
        uid = UID(self.address)

        payload = request.get_bytes()
        pending_request = PendingRequest(dest_addr, uid, payload, onResponse, onFail)
        self.pending_requests[str(uid)] = pending_request
        self.send_to(uid, payload, dest_addr)

    def send_response(self, uid, response, dest_address):
        self.reply(uid, response.get_bytes(), dest_address)

    def reply(self, uid, payload, dest_address):
        self.add_to_response_cache(uid, payload)
        self.send_to(uid, payload, dest_address)

    def send_to(self, uid, payload, addr):
        self.socket.sendto(str(uid) + payload, addr)

    def add_to_response_cache(self, uid, data):
        if len(self.response_cache) < Sender.MAX_RESPONSE_CACHE_SIZE:
            self.response_cache[str(uid)] = CacheItem(data)