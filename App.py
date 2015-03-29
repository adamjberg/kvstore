from __future__ import with_statement
import hashlib
import socket
import sys
import time
from KVStore import KVStore
from Message import Message
from Node import Node
from Response import *
from Request import *
from UDPClient import UDPClient
from UID import UID

class App:

    def __init__(self):
        self.nodes = []
        self.init_nodes_from_file()

        if self.init_client() is False:
            print "Failed to bind to a port."
            sys.exit()

        self.kvStore = KVStore()

        self.send_set_online_request()

    def init_nodes_from_file(self):
        with open("hosts.txt") as f:
            lines = [x.strip('\n') for x in f.readlines()]
        for line in lines:
            host, port, location = line.split(":")
            self.nodes.append(Node(host, port, location))

    def init_client(self):
        nodes_for_my_ip = []

        for node in self.nodes:
            ip = node.ip
            if self.does_ip_match_mine(ip):
                nodes_for_my_ip.append(node)

        for node in nodes_for_my_ip:
            try:
                self.client = UDPClient(node.port, self.handle_message)
                self.my_node = node
                print "Connected on port: " + str(node.port)
                return True
            except socket.error:
                pass

        return False

    def does_ip_match_mine(self, ip):
        if ip.startswith("127.") or ip.startswith("localhost"):
            return True
        elif ip == socket.gethostbyname(socket.gethostname()):
            return True
        elif ip == socket.gethostname():
            return True
        return False

    def handle_put_request(self, message, request):
        if self.kvStore.put(request.key, request.value):
            response = SuccessResponse()
        else:
            response = OutOfSpaceResponse()
        self.client.send_response(message, response)
        return None

    def handle_get_request(self, message, request):
        value = self.kvStore.get(request.key)
        if value:
            response = SuccessResponse(value)
        else:
            response = NonexistentKeyResponse()

        self.client.send_response(message, response)

    def handle_remove_request(self, message, request):
        if self.kvStore.remove(request.key):
            response = SuccessResponse()
        else:
            response = NonexistentKeyResponse()

        self.client.send_response(message, response)

    def handle_shutdown_request(self, message, request):
        self.client.send_response(message, SuccessResponse())
        sys.exit()

    def handle_incoming_forwarded_request(self, message, request):
        original_request = request.original_request

        uid = request.original_uid
        payload = original_request.get_bytes()
        handle_message(Message(uid, payload, request.return_addr))
        handle_message(Message(message.uid, payload, message.sender_addr))

    def handle_set_online_request(self, message, request):
        reset_pending_requests_for_addr(message.sender_addr)
        set_node_online_with_addr(message.sender_addr, True)
        self.client.send_response(message, SuccessResponse())

    def reset_pending_requests_for_addr(self, addr):
        for uid, request in self.client.pending_requests.items():
            if(request.dest_addr == addr):
                request.reset()


    def handle_message(self, message):
        request = Request.from_bytes(message.payload)

        if request is None:
            self.client.send_response(message, UnrecognizedCommandResponse())
            return

        if isinstance(request,ForwardedRequest):
            handle_incoming_forwarded_request(message, request)
            return

        if request.command == ShutdownRequest.COMMAND:
            handle_shutdown_request(request)
        elif request.command == SetOnlineRequest.COMMAND:
            handle_set_online_request(message, request)
            return

        dest_node = get_responsible_node_for_key(request.key)

        if dest_node == my_node:
            if request.command == PutRequest.COMMAND:
                handle_put_request(message, request)
            elif request.command == GetRequest.COMMAND:
                handle_get_request(message, request)
            elif request.command == RemoveRequest.COMMAND:
                handle_remove_request(message, request)
        else:
            forward_request(message, request, dest_node)

    def forward_request(self, message, original_request, dest_node):
        request = ForwardedRequest(message.uid, message.sender_addr, original_request)
        self.client.send_request(request, dest_node.get_addr(), forward_succeeded, forward_failed)

    def forward_succeeded(self, message):
        pass

    def forward_failed(self, failed_udpclient_request):
        for node in self.nodes:
            if node.get_addr() == failed_udpclient_request.dest_addr:
                node.online = False

        failed_request = Request.from_bytes(failed_udpclient_request.payload)
        original_request = failed_request.original_request

        uid = failed_request.original_uid
        payload = original_request.get_bytes()
        message = Message(uid, payload, failed_request.return_addr)

        handle_message(message)

    def get_responsible_node_for_key(self, key):
        dest_node = None
        location = get_location_for_key(key)
        for node in self.nodes:
            if node.online == False:
                continue

            if location >= node.location:
                dest_node = node

        if dest_node is None:
            dest_node = my_node

        return dest_node

    def get_location_for_key(self, key):
        return struct.unpack('B', hashlib.sha256(key).digest()[0])[0]

    def send_set_online_request(self):
        request = SetOnlineRequest()
        for node in self.nodes:
            if node != self.my_node:
                self.client.send_request(request, node.get_addr(), set_online_success, set_online_failed)

    def set_online_success(self, message):
        self.set_node_online_with_addr(message.sender_addr, True)

    def set_online_failed(self, request):
        self.set_node_online_with_addr(request.dest_addr, False)

    def set_node_online_with_addr(self, addr, online):
        for node in self.nodes:
            if node.get_addr() == addr:
                node.online = online
                break

if __name__ == "__main__":
    App()