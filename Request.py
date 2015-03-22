import struct

class Request:
    PUT = 1
    GET = 2
    REMOVE = 3
    SHUTDOWN = 4

    COMMAND_LENGTH_BYTES = 1
    KEY_LENGTH_BYTES = 32
    VALUE_LENGTH_START_POS = COMMAND_LENGTH_BYTES + KEY_LENGTH_BYTES
    VALUE_LENGTH_BYTES = 2
    VALUE_LENGTH_END_POS = VALUE_LENGTH_START_POS + VALUE_LENGTH_BYTES
    MAX_VALUE_LENGTH = 15000

    def __init__(self, command, key, value):
        self.command = ord(command)
        self.key = key
        self.value = value

    def is_key_valid(self):
        return len(self.key) == Request.KEY_LENGTH_BYTES

    def get_bytes(self):
        return struct.pack("<c%dsH" % (Request.KEY_LENGTH_BYTES), self.command, self.key, len(self.value)) + value

    def __str__(self):
        return str(self.command) + str(self.key) + str(self.value)

    @staticmethod
    def from_bytes(b):
        if len(b) == 0:
            raise Exception("Empty bytes trying to be parsed as Request")

        command = struct.unpack("<c",b[0])[0]

        if len(b) > Request.VALUE_LENGTH_START_POS:
            key = struct.unpack(
                "%ds" % (Request.KEY_LENGTH_BYTES),
                b[1:Request.VALUE_LENGTH_START_POS]
            )[0]
        else:
            key = ""

        if(len(b) > Request.VALUE_LENGTH_START_POS):
            value_length = struct.unpack(
                "<H",
                b[Request.VALUE_LENGTH_START_POS:Request.VALUE_LENGTH_END_POS]
            )[0]
            if value_length > 0:
                if value_length <= len(b[Request.VALUE_LENGTH_END_POS:]):
                    value = b[Request.VALUE_LENGTH_END_POS:Request.VALUE_LENGTH_END_POS + value_length]
                else:
                    raise Exception("Value Length Too Big %d %d" % (value_length, len(b)))
            else:
                value = ""
        else:
            value = ""

        return Request(command, key, value)
