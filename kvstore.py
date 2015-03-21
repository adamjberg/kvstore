import random
import socket
import struct
import time

PORT = 12000

class UID:
    LENGTH = 16
    def __init__(self):
        self.ip = socket.gethostbyname(socket.gethostname())
        self.port = PORT
        self.random = random.getrandbits(16)
        self.timestamp = time.time()

    def get_bytes(self):
        return struct.pack("<IHHQ",
            socket.inet_aton(self.ip)[0],
            self.port,
            random.getrandbits(8),
            int(round(self.timestamp * 1000)))

    @staticmethod
    def from_bytes(b):
        if len(b) < LENGTH:
            return None

        uid = UID();
        uid.ip, uid.port, uid.random, uid.timestamp = struct.unpack("<IHHQ", b[:LENGTH])
        return uid

class Message:
    def __init__(self, uid, payload, sender_addr):
        self.uid = uid
        self.payload = payload
        self.sender_addr = sender_addr

    def get_bytes(self):
        return self.uid + self.payload

    def __str__(self):
        return str(self.uid) + str(self.payload) + str(self.sender_addr)

class Request:
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
        self.socket.setblocking(0)
        self.pending_requests = {}
        self.cached_responses = {}

    def receive(self):
        try:
            data, addr = self.socket.recv(UDPClient.MAX_LENGTH)
            uid = UID.from_bytes(data)
            payload = data[UID.LENGTH:]
            message = Message(uid, payload, addr)

            if self.cached_responses[uid] is not None:
                self.reply(message, self.cached_responses[uid])
                print "CACHED"
                return None

            if self.pending_requests[uid] is not None:
                onResponse = self.pending_requests[uid].onResponse
                if onResponse is not None and hasattr(onResponse, '__call__'):
                    onResponse(message.payload)

            return message
        except:
            pass

    def sendRequest(self, payload, addr, onResponse, onFail):
        uid = UID()
        self.pending_requests[uid] = Request(payload, onResponse, onFail)
        self.sendTo(uid, payload, addr)

    def reply(self, message, payload):
        self.cached_responses[message.uid] = payload
        self.sendTo(message.uid, payload, message.sender_addr)

    def sendTo(self, uid, payload, addr):
        self.socket.sendto(uid + payload, addr)

if __name__ == "__main__":
    client = UDPClient(PORT)
    client.receive()
