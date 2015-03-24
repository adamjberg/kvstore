class Node:
    def __init__(self, ip, port, location):
        self.ip = ip
        self.port = int(port)
        self.location = int(location)
        self.online = True

    def get_addr(self):
        return (self.ip, self.port)