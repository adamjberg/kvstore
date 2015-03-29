import socket
import struct
from UID import UID

class Request:
    COMMAND_LENGTH_BYTES = 1
    KEY_LENGTH_BYTES = 32
    VALUE_LENGTH_START_POS = COMMAND_LENGTH_BYTES + KEY_LENGTH_BYTES
    VALUE_LENGTH_BYTES = 2
    VALUE_LENGTH_END_POS = VALUE_LENGTH_START_POS + VALUE_LENGTH_BYTES
    MAX_VALUE_LENGTH = 15000

    def __init__(self, command):
        self.command = command

    def is_key_valid(self):
        return len(self.key) == Request.KEY_LENGTH_BYTES

    def get_bytes(self):
        return struct.pack("<c", self.command)

    def __str__(self):
        return str(self.command)

    @staticmethod
    def from_bytes(b):
        if len(b) == 0:
            raise Exception("Empty bytes trying to be parsed as Request")

        command = struct.unpack("<c",b[0])[0]

        if command == PutRequest.COMMAND:
            key = Request.get_key_from_bytes(b)
            value = Request.get_value_from_bytes(b)
            return PutRequest(key,value)
        elif command == GetRequest.COMMAND:
            key = Request.get_key_from_bytes(b)
            req = GetRequest(key)
            return req
        elif command == RemoveRequest.COMMAND:
            return RemoveRequest(Request.get_key_from_bytes(b))
        elif command == ShutdownRequest.COMMAND:
            return ShutdownRequest()
        elif command == InternalPutRequest.COMMAND:
            key = Request.get_key_from_bytes(b)
            value = Request.get_value_from_bytes(b)
            return InternalPutRequest(key, value)
        elif command == InternalGetRequest.COMMAND:
            key = Request.get_key_from_bytes(b)
            return InternalGetRequest(key)
        elif command == InternalRemoveRequest.COMMAND:
            key = Request.get_key_from_bytes(b)
            return InternalRemoveRequest(key)
        elif command == JoinRequest.COMMAND:
            return JoinRequest()
        elif command == JoinSuccessRequest.COMMAND:
            return JoinSuccessRequest()
        elif command == SetOnlineRequest.COMMAND:
            return SetOnlineRequest()
        elif command == SetOfflineRequest.COMMAND:
            ip = struct.unpack("<I", b[1:5])[0]
            ip = socket.inet_ntoa(struct.pack("!I", ip))
            port = struct.unpack("<H", b[5:])[0]
            return SetOfflineRequest((ip, port))
        elif command == PingRequest.COMMAND:
            return PingRequest()
        elif command == ForwardedRequest.COMMAND:
            original_uid = UID.from_bytes(b[1:UID.LENGTH + 1])
            ip = struct.unpack("<I", b[UID.LENGTH + 1: UID.LENGTH + 1 + 4])[0]
            ip = socket.inet_ntoa(struct.pack("!I", ip))
            port = struct.unpack("<H", b[UID.LENGTH + 1 + 4:UID.LENGTH + 1 + 6])[0]
            return ForwardedRequest(original_uid, (ip, port), Request.from_bytes(b[UID.LENGTH + 1 + 6:]))
        else:
            return UnrecognizedRequest()

    @staticmethod
    def get_key_from_bytes(b):
        if len(b) >= Request.VALUE_LENGTH_START_POS:
            return struct.unpack(
                "%ds" % (Request.KEY_LENGTH_BYTES),
                b[1:Request.VALUE_LENGTH_START_POS]
            )[0]
        else:
            return ""

    @staticmethod
    def get_value_from_bytes(b):
        if(len(b) > Request.VALUE_LENGTH_START_POS):
            value_length = struct.unpack(
                "<H",
                b[Request.VALUE_LENGTH_START_POS:Request.VALUE_LENGTH_END_POS]
            )[0]
            if value_length > 0:
                if value_length <= len(b[Request.VALUE_LENGTH_END_POS:]):
                    return b[Request.VALUE_LENGTH_END_POS:Request.VALUE_LENGTH_END_POS + value_length]
                else:
                    raise Exception("Value Length Too Big %d %d" % (value_length, len(b)))
            else:
                return ""
        else:
            return ""

class UnrecognizedRequest(Request):
    COMMAND = chr(99)
    def __init__(self):
        Request.__init__(self, PutRequest.COMMAND)

class PutRequest(Request):
    COMMAND = chr(1)
    def __init__(self, key, value):
        Request.__init__(self, PutRequest.COMMAND)
        self.key = key
        self.value = value

    def get_bytes(self):
        return Request.get_bytes(self) + struct.pack("<%dsH" % (Request.KEY_LENGTH_BYTES), self.key, len(self.value)) + self.value

class GetRequest(Request):
    COMMAND = chr(2)
    def __init__(self, key):
        Request.__init__(self, GetRequest.COMMAND)
        self.key = key

    def get_bytes(self):
        return Request.get_bytes(self) + struct.pack("<%ds" % (Request.KEY_LENGTH_BYTES), self.key)

class RemoveRequest(Request):
    COMMAND = chr(3)
    def __init__(self, key):
        Request.__init__(self, RemoveRequest.COMMAND)
        self.key = key

    def get_bytes(self):
        return Request.get_bytes(self) + struct.pack("<%ds" % (Request.KEY_LENGTH_BYTES), self.key)

class ShutdownRequest(Request):
    COMMAND = chr(4)
    def __init__(self):
        Request.__init__(self, ShutdownRequest.COMMAND)

class InternalPutRequest(Request):
    COMMAND = chr(41)
    def __init__(self):
        Request.__init__(self, InternalPutRequest.COMMAND)

class InternalGetRequest(Request):
    COMMAND = chr(42)
    def __init__(self):
        Request.__init__(self, InternalGetRequest.COMMAND)

class InternalRemoveRequest(Request):
    COMMAND = chr(43)
    def __init__(self):
        Request.__init__(self, InternalRemoveRequest.COMMAND)

class JoinRequest(Request):
    COMMAND = chr(44)
    def __init__(self):
        Request.__init__(self, JoinRequest.COMMAND)

class JoinSuccessRequest(Request):
    COMMAND = chr(45)
    def __init__(self):
        Request.__init__(self, JoinSuccessRequest.COMMAND)

class SetOnlineRequest(Request):
    COMMAND = chr(46)
    def __init__(self):
        Request.__init__(self, SetOnlineRequest.COMMAND)

class SetOfflineRequest(Request):
    COMMAND = chr(47)
    def __init__(self, addr):
        Request.__init__(self, SetOfflineRequest.COMMAND)
        self.addr = addr

    def get_bytes(self):
        return Request.get_bytes(self) + struct.pack("<IH", struct.unpack("!I", socket.inet_aton(self.addr[0]))[0], self.addr[1])

class PingRequest(Request):
    COMMAND = chr(48)
    def __init__(self):
        Request.__init__(self, PingRequest.COMMAND)

class ForwardedRequest(Request):
    COMMAND = chr(49)
    def __init__(self, original_uid, return_addr, request):
        Request.__init__(self, ForwardedRequest.COMMAND)
        self.original_uid = original_uid
        self.return_addr = return_addr
        self.original_request = request

    def get_bytes(self):
        return Request.get_bytes(self) + \
        self.original_uid.get_bytes() + \
        struct.pack("<IH", struct.unpack("!I", socket.inet_aton(self.return_addr[0]))[0], self.return_addr[1]) + \
        self.original_request.get_bytes()