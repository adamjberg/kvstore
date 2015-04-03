import struct

class Response:
    SUCCESS = chr(0)
    NON_EXISTENT = chr(1)
    OUT_OF_SPACE = chr(2)
    SYSTEM_OVERLOAD = chr(3)
    KV_STORE_FAILURE = chr(4)
    UNRECOGNIZED_COMMAND = chr(5)

    def __init__(self, code, value = ""):
        self.code = code
        self.value = value

    def get_bytes(self):
        return struct.pack("<cH", self.code, len(self.value)) + self.value

class SuccessResponse(Response):
    def __init__(self, value = ""):
        Response.__init__(self, Response.SUCCESS, value)

class NonexistentKeyResponse(Response):
    def __init__(self):
        Response.__init__(self, Response.NON_EXISTENT)

class OutOfSpaceResponse(Response):
    def __init__(self):
        Response.__init__(self, Response.OUT_OF_SPACE)

class KeyValueStoreFailureResponse(Response):
    def __init__(self):
        Response.__init__(self, Response.KV_STORE_FAILURE)

class UnrecognizedCommandResponse(Response):
    def __init__(self):
        Response.__init__(self, Response.UNRECOGNIZED_COMMAND)