class Node:
    def __init__(self, ip, port, location):
        self.ip = ip
        self.port = int(port)
        self.location = int(location)
        self.online = True

    def get_addr(self):
        return (self.ip, self.port)

    def __str__(self):
        return str(self.ip) + " " + str(self.port) + " " + str(self.location) + " " + str(self.online)