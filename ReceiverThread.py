from threading import Thread

class ReceiverThread(Thread):
    MAX_LENGTH = 16000

    def __init__(self, socket, received_data_queue):
        Thread.__init__(self)
        self.daemon = True
        self.socket = socket
        self.received_data_queue = received_data_queue

    def run(self):
        while True:
            self.received_data_queue.put(self.socket.recvfrom(ReceiverThread.MAX_LENGTH))