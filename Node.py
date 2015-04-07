import socket

class Node:
    def __init__(self, hostname, port, location):
        self.hostname = hostname
        
        try:
            self.ip = socket.gethostbyname(hostname)
        except:
            print "Failed to get ip for " + hostname
            self.ip = hostname

        self.port = int(port)
        self.location = int(location)
        self.online = True

    def get_addr(self):
        return (self.ip, self.port)

    def __str__(self):
        return str(self.ip) + " " + str(self.port) + " " + str(self.location) + " " + str(self.online)