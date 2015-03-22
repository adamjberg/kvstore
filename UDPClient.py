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
                        del self.client.pending_requests[uid]
                        continue

                    request.attempts += 1
                    request.timeout *= 2

                    self.client.sendTo(request.uid, request.payload, request.dest_addr)
                    request.last_attempt_time = cur_time

            time.sleep(UDPClient.DEFAULT_TIMEOUT_IN_MS * 0.001)

class UDPClient:
    MAX_LENGTH = 65535
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
            payload = data[UID.LENGTH:]
            message = Message(uid, payload, addr)

            uidHash = uid.get_hash()

            cached_response = self.response_cache.get(uidHash)
            if cached_response:
                self.reply(message, cached_response)
                return None

            if uidHash in self.pending_requests:
                onResponse = self.pending_requests[uidHash].onResponse
                if onResponse is not None and hasattr(onResponse, '__call__'):
                    onResponse(message.payload)

            return message
        except socket.error:
            pass

    def send_request(self, message, addr, onResponse, onFail):
        uid = UID(self.port)
        payload = message.payload
        self.pending_requests[uid.get_hash()] = UDPClientRequest(message.sender_addr, addr, uid, payload, onResponse, onFail)
        self.sendTo(uid, payload, addr)

    def send_response(self, message, response):
        self.reply(message, response.get_bytes())

    def reply(self, message, payload):
        self.response_cache[message.uid.get_hash()] = payload
        self.sendTo(message.uid, payload, message.sender_addr)

    def sendTo(self, uid, payload, addr):
        self.socket.sendto(uid.get_bytes() + str(payload), addr)