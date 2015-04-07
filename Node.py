import socket

class Node:
    def __init__(self, hostname, port, location):
        self.hostname = hostname
        self.ip = socket.gethostbyname(hostname)
        self.port = int(port)
        self.location = int(location)
        self.online = True

    def get_addr(self):
        return (self.ip, self.port)

    def __str__(self):
        return str(self.ip) + " " + str(self.port) + " " + str(self.location) + " " + str(self.online)