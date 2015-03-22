class Message:
    def __init__(self, uid, payload, sender_addr):
        self.uid = uid
        self.payload = payload
        self.sender_addr = sender_addr

    def get_bytes(self):
        return self.uid.get_bytes() + str(self.payload)