import socket

class Node:
    NUM_RTTS_TO_STORE = 10

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
        self.average_rtt = 0
        self.rtts = []

    def get_addr(self):
        return (self.ip, self.port)

    def update_rtt_stats(self, new_rtt):
        if len(self.rtts) > Node.NUM_RTTS_TO_STORE:
            self.rtts.pop()
        self.rtts.append(new_rtt)
        self.average_rtt = sum(self.rtts) / len(self.rtts)

    def __str__(self):
        return str(self.ip) + " " + str(self.port) + " " + str(self.location) + " " + str(self.online)