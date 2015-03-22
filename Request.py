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
        return struct.pack("<c32sH", self.command, self.key, len(self.value)) + value

    def __str__(self):
        return str(self.command) + str(self.key) + str(self.value)

    @staticmethod
    def from_bytes(b):
        command, key = struct.unpack(
            "<c%ds" % Request.KEY_LENGTH_BYTES,
            b[:Request.VALUE_LENGTH_START_POS]
        )
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

        return Request(command, key, value)
