from expiringdict import ExpiringDict
import socket
import time
from threading import Thread
from UID import UID
from Message import Message

class UDPClientRequest:
    def __init__(self, source_addr, dest_addr, uid, payload, onResponse, onFail):
        self.source_addr = source_addr
        self.dest_addr = dest_addr
        self.uid = uid
        self.payload = payload
        self.onResponse = onResponse
        self.onFail = onFail
        self.attempts = 0
        self.timeout = UDPClient.DEFAULT_TIMEOUT_IN_MS
        self.last_attempt_time = time.time()

class RequestTimeoutThread(Thread):
    def __init__(self, client):
        Thread.__init__(self)
        self.client = client
        self.daemon = True

    def run(self):
        while True:
            cur_time = time.time()
            for uid, request in self.client.pending_requests.items():
                time_since_last_attempt = (cur_time - request.last_attempt_time) * 1000

                if time_since_last_attempt > request.timeout:
                    if request.attempts >= UDPClient.MAX_RETRY_ATTEMPTS:
                        request.onFail(request)
                        self.client.handled_request_cache[uid] = request
                        del self.client.pending_requests[uid]
                        continue

                    request.attempts += 1
                    request.timeout *= 2

                    self.client.sendTo(request.uid, request.payload, request.dest_addr)
                    request.last_attempt_time = cur_time

            time.sleep(UDPClient.DEFAULT_TIMEOUT_IN_MS * 0.001)

class UDPClient:
    MAX_LENGTH = 16000
    MAX_RETRY_ATTEMPTS = 3
    MAX_CACHE_LENGTH = 1000
    DEFAULT_TIMEOUT_IN_MS = 100
    CACHE_EXPIRATION_TIME_SECONDS = 5

    def __init__(self, port):
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(("localhost", self.port))
        self.socket.setblocking(False)
        self.pending_requests = dict()
        self.handled_request_cache = ExpiringDict(max_len = UDPClient.MAX_CACHE_LENGTH, max_age_seconds = UDPClient.CACHE_EXPIRATION_TIME_SECONDS)
        self.response_cache = ExpiringDict(max_len = UDPClient.MAX_CACHE_LENGTH, max_age_seconds = UDPClient.CACHE_EXPIRATION_TIME_SECONDS)
        self.request_timeout_thread = RequestTimeoutThread(self)
        self.request_timeout_thread.start()

    def receive(self):
        try:
            data, addr = self.socket.recvfrom(UDPClient.MAX_LENGTH)

            if(addr == self.socket.getsockname()):
                raise Exception("You are sending to yourself")
                return None

            uid = UID.from_bytes(data)

            if(int(round(time.time() * 1000)) - uid.timestamp > UDPClient.CACHE_EXPIRATION_TIME_SECONDS * 1000):
                return None

            payload = data[UID.LENGTH:]
            message = Message(uid, payload, addr)
            uidBytes = uid.get_bytes()

            if uidBytes in self.handled_request_cache:
                return None

            cached_response = self.response_cache.get(uidBytes)
            if cached_response:
                self.reply(message, cached_response)
                return None

            if uidBytes in self.pending_requests:
                successful_request = self.pending_requests[uidBytes]
                onResponse = successful_request.onResponse
                if onResponse is not None and hasattr(onResponse, '__call__'):
                    onResponse(message.payload)
                self.handled_request_cache[uidBytes] = successful_request
                del self.pending_requests[uidBytes]

                return None

            return message
        except socket.error:
            pass

    def send_request(self, request, sender_addr, dest_addr, onResponse, onFail):
        uid = UID(self.port)

        payload = request.get_bytes()
        self.pending_requests[uid.get_bytes()] = UDPClientRequest(sender_addr, dest_addr, uid, payload, onResponse, onFail)
        self.sendTo(uid, payload, dest_addr)

    def send_response(self, message, response):
        self.reply(message, response.get_bytes())

    def reply(self, message, payload):
        self.response_cache[message.uid.get_bytes()] = payload
        self.sendTo(message.uid, payload, message.sender_addr)

    def sendTo(self, uid, payload, addr):
        self.socket.sendto(uid.get_bytes() + payload, addr)