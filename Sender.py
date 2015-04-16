import socket
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

class Sender:
    MAX_RETRY_ATTEMPTS = 3
    DEFAULT_TIMEOUT_IN_MS = 100

    def __init__(self, sock):
        self.socket = sock
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
        uid_bytes = sr(request.uid)

        if request.onFail is not None and hasattr(request.onFail, '__call__'):
            request.onFail(request)

        self.handled_request_cache.put(uid_bytes, request)
        del self.pending_requests[uid_bytes]

    def check_cached_responses(self, uid, sender_address):
        try:
            cached_response = self.response_cache[str(uid)]
            self.send_to(uid, cached_response, sender_address)
            return True
        except:
            return False

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

    def send_request(self, request, dest_addr, onResponse = None, onFail = None):
        uid = UID(self.addr)

        payload = request.get_bytes()
        pending_request = PendingRequest(dest_addr, uid, payload, onResponse, onFail)
        self.pending_requests[str(uid)] = pending_request
        self.send_to(uid, payload, dest_addr)

    def send_response(self, uid, response, dest_address):
        self.reply(uid, response.get_bytes(), dest_address)

    def reply(self, uid, payload, dest_address):
        self.response_cache[str(uid)] = payload
        self.send_to(uid, payload, dest_address)

    def send_to(self, uid, payload, addr):
        self.socket.sendto(str(uid) + payload, addr)