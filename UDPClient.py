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
    def __init__(self, port):
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(("localhost", self.port))
        self.socket.setblocking(False)
        self.pending_requests = dict()
        self.cached_responses = dict()

    def receive(self):
        try:
            data, addr = self.socket.recvfrom(UDPClient.MAX_LENGTH)
            uid = UID.from_bytes(data)
            payload = data[UID.LENGTH:]
            message = Message(uid, payload, addr)

            if str(uid) in self.cached_responses:
                print "CACHE"
                self.reply(message, self.cached_responses[str(uid)])
                return None

            if str(uid) in self.pending_requests:
                onResponse = self.pending_requests[str(uid)].onResponse
                if onResponse is not None and hasattr(onResponse, '__call__'):
                    onResponse(message.payload)

            return message
        except socket.error:
            pass

    def send_request(self, payload, addr, onResponse, onFail):
        uid = UID(self.port)
        self.pending_requests[uid] = UDPClientRequest(payload, onResponse, onFail)
        self.sendTo(uid, payload, addr)

    def send_response(self, message, response):
        self.reply(message, response.get_bytes())

    def reply(self, message, payload):
        self.cached_responses[str(message.uid)] = payload
        self.sendTo(message.uid, payload, message.sender_addr)

    def sendTo(self, uid, payload, addr):
        self.socket.sendto(str(uid) + str(payload), addr)
