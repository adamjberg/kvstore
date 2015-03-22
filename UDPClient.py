from expiringdict import ExpiringDict
import socket
from UID import UID
from Message import Message

class UDPClientRequest:
    def __init__(self, payload, onResponse, onFail):
        self.payload = payload
        self.onResponse = onResponse
        self.onFail = onFail

class UDPClient:
    MAX_LENGTH = 65535
    MAX_CACHE_LENGTH = 1000
    CACHE_EXPIRATION_TIME_SECONDS = 5

    def __init__(self, port):
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(("localhost", self.port))
        self.socket.setblocking(False)
        self.pending_requests = dict()
        self.response_cache = ExpiringDict(max_len = UDPClient.MAX_CACHE_LENGTH, max_age_seconds = UDPClient.CACHE_EXPIRATION_TIME_SECONDS)

    def receive(self):
        try:
            data, addr = self.socket.recvfrom(UDPClient.MAX_LENGTH)
            uid = UID.from_bytes(data)
            payload = data[UID.LENGTH:]
            message = Message(uid, payload, addr)

            uidHash = uid.get_hash()

            cached_response = self.response_cache.get(uidHash)
            if cached_response:
                print "CACHE"
                self.reply(message, cached_response)
                return None

            if uidHash in self.pending_requests:
                onResponse = self.pending_requests[uidHash].onResponse
                if onResponse is not None and hasattr(onResponse, '__call__'):
                    onResponse(message.payload)

            return message
        except socket.error:
            pass

    def send_request(self, payload, addr, onResponse, onFail):
        uid = UID(self.port)
        self.pending_requests[uid.get_hash()] = UDPClientRequest(payload, onResponse, onFail)
        self.sendTo(uid, payload, addr)

    def send_response(self, message, response):
        self.reply(message, response.get_bytes())

    def reply(self, message, payload):
        self.response_cache[message.uid.get_hash()] = payload
        self.sendTo(message.uid, payload, message.sender_addr)

    def sendTo(self, uid, payload, addr):
        self.socket.sendto(uid.get_bytes() + str(payload), addr)
