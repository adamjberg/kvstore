import hashlib
import random
import socket
import struct
import time

class UID:
    LENGTH = 16
    def __init__(self, port = None):
        self.ip = struct.unpack("<I", socket.inet_aton(socket.gethostbyname(socket.gethostname())))[0]
        self.port = port
        self.random = random.getrandbits(16)
        self.timestamp = int(round(time.time() * 1000))

    def get_hash(self):
        return hashlib.sha256(self.get_bytes()).hexdigest()

    def get_bytes(self):
        return struct.pack("<IHHQ",
            self.ip,
            self.port,
            self.random,
            self.timestamp)

    @staticmethod
    def from_bytes(b):
        if len(b) < UID.LENGTH:
            raise Exception("Invalid UID") 

        uid = UID();
        uid.ip, uid.port, uid.random, uid.timestamp = struct.unpack("<IHHQ", b[:UID.LENGTH])
        return uid