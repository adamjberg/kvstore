import struct

SUCCESS = 0
NON_EXISTENT = 1
OUT_OF_SPACE = 2
SYSTEM_OVERLOAD = 3
KV_STORE_FAILURE = 4
UNRECOGNIZED_COMMAND = 5

class Response:
    def __init__(self, code, value = ""):
        self.code = code
        self.value = value

    def get_bytes(self):
        return struct.pack("<cH", chr(self.code), len(self.value)) + self.value

class SuccessResponse(Response):
    def __init__(self, value = ""):
        Response.__init__(self, SUCCESS, value)

class NonexistentKeyResponse(Response):
    def __init__(self):
        Response.__init__(self, NON_EXISTENT)

class OutOfSpaceResponse(Response):
    def __init__(self):
        Response.__init__(self, OUT_OF_SPACE)

class KeyValueStoreFailureResponse(Response):
    def __init__(self):
        Response.__init__(self, KV_STORE_FAILURE)

class UnrecognizedCommandResponse(Response):
    def __init__(self):
        Response.__init__(self, UNRECOGNIZED_COMMAND)