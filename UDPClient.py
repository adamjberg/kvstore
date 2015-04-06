from GenericCache.GenericCache import GenericCache
import os
import socket
import sys
import time
from threading import Thread
from UID import UID
from Message import Message

class UDPClientRequest:
    def __init__(self, dest_addr, uid, payload, onResponse, onFail):
        self.dest_addr = dest_addr
        self.uid = uid
        self.payload = payload
        self.onResponse = onResponse
        self.onFail = onFail
        self.attempts = 0
        self.timeout = UDPClient.DEFAULT_TIMEOUT_IN_MS
        self.last_attempt_time = time.time()

    def reset(self):
        self.attempts = 0
        self.timeout = UDPClient.DEFAULT_TIMEOUT_IN_MS
        self.last_attempt_time = 0

class RequestTimeoutThread(Thread):
    def __init__(self, client):
        Thread.__init__(self)
        self.client = client
        self.daemon = True

    def run(self):
        while True:
            try:
                cur_time = time.time()
                failed_dest_addresses = {}
                for uid, request in self.client.pending_requests.items():
                    time_since_last_attempt = (cur_time - request.last_attempt_time) * 1000

                    if time_since_last_attempt > request.timeout:
                        if request.attempts >= UDPClient.MAX_RETRY_ATTEMPTS:
                            failed_dest_addresses[request.dest_addr] = True
                            continue

                        request.attempts += 1
                        request.timeout *= 2

                        self.client.sendTo(request.uid, request.payload, request.dest_addr)
                        request.last_attempt_time = cur_time

                failed_requests = []
                for addr in failed_dest_addresses:
                    for key, request in self.client.pending_requests.items():
                        if request.dest_addr == addr:
                            self.fail_request(request)

                time.sleep(UDPClient.DEFAULT_TIMEOUT_IN_MS * 0.001)
            except:
                pass

    def fail_request(self, request):
        uid_bytes = request.uid.get_bytes()

        if request.onFail is not None and hasattr(request.onFail, '__call__'):
            request.onFail(request)

        self.client.handled_request_cache.insert(uid_bytes, request)
        del self.client.pending_requests[uid_bytes]  

class MessageDispatcherThread(Thread):
    def __init__(self, client):
        Thread.__init__(self)
        self.client = client
        self.daemon = True

    def run(self):
        while True:
            try:
                if len(self.client.received_data) == 0:
                    time.sleep(0.0001)
                    continue

                data, addr = self.client.received_data.pop(0)
                uid = UID.from_bytes(data)

                if(int(round(time.time() * 1000)) - uid.timestamp > UDPClient.CACHE_EXPIRATION_TIME_SECONDS * 1000):
                    continue

                payload = data[UID.LENGTH:]
                message = Message(uid, payload, addr)
                uidBytes = uid.get_bytes()

                if uidBytes in self.client.handled_request_cache:
                    continue

                cached_response = self.client.response_cache.fetch(uidBytes)
                if cached_response:
                    self.client.reply(message, cached_response)
                    continue

                if uidBytes in self.client.pending_requests:
                    successful_request = self.client.pending_requests[uidBytes]
                    onResponse = successful_request.onResponse
                    if onResponse is not None and hasattr(onResponse, '__call__'):
                        onResponse(message)
                    self.client.handled_request_cache.insert(uidBytes, successful_request)
                    del self.client.pending_requests[uidBytes]
                    continue

                if self.client.on_message_received:
                    self.client.on_message_received(message)
            except:
                pass


class UDPClient:
    MAX_LENGTH = 16000
    MAX_RETRY_ATTEMPTS = 3
    MAX_CACHE_LENGTH = 1000
    DEFAULT_TIMEOUT_IN_MS = 100
    CACHE_EXPIRATION_TIME_SECONDS = 5

    def __init__(self, addr, on_message_received = None):
        self.addr = addr
        self.on_message_received = on_message_received

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(addr)
        self.socket.setblocking(True)

        self.pending_requests = dict()
        self.handled_request_cache = GenericCache(UDPClient.MAX_CACHE_LENGTH, UDPClient.CACHE_EXPIRATION_TIME_SECONDS)
        self.response_cache = GenericCache(UDPClient.MAX_CACHE_LENGTH, UDPClient.CACHE_EXPIRATION_TIME_SECONDS)

        self.request_timeout_thread = RequestTimeoutThread(self)
        self.request_timeout_thread.start()

        self.received_data = []

        self.message_dispatcher_thread = MessageDispatcherThread(self)
        self.message_dispatcher_thread.start()

    def run(self):
        while True:
            try:
                self.received_data.append(self.socket.recvfrom(UDPClient.MAX_LENGTH))
            except KeyboardInterrupt:
                os._exit(os.EX_OK)
                break
            except:
                pass

    def send_request(self, request, dest_addr, onResponse = None, onFail = None):
        uid = UID(self.addr)

        payload = request.get_bytes()
        client_request = UDPClientRequest(dest_addr, uid, payload, onResponse, onFail)
        self.pending_requests[uid.get_bytes()] = client_request
        self.sendTo(uid, payload, dest_addr)
        return client_request

    def send_response(self, message, response):
        self.reply(message, response.get_bytes())

    def reply(self, message, payload):
        self.response_cache.insert(message.uid.get_bytes(), payload)
        self.sendTo(message.uid, payload, message.sender_addr)

    def sendTo(self, uid, payload, addr):
        self.socket.sendto(uid.get_bytes() + payload, addr)