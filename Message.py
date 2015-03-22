class Message:
    def __init__(self, uid, payload, sender_addr):
        self.uid = uid
        self.payload = payload
        self.sender_addr = sender_addr

    def get_bytes(self):
        return self.uid + self.payload

    def __str__(self):
        return str(self.uid) + str(self.payload) + str(self.sender_addr)