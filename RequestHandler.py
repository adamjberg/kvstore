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
from RequestHandler import *

class RequestHandler:
    def __init__(self, client, kvStore, node_circle):
        self.client = client
        self.kvStore = kvStore
        self.node_circle = node_circle
        self.my_node = node_circle.my_node

        self.handlers = {
            PutRequest.COMMAND: self.handle_put,
            GetRequest.COMMAND: self.handle_get,
            RemoveRequest.COMMAND: self.handle_remove,
            InternalPutRequest.COMMAND: self.handle_put,
            InternalGetRequest.COMMAND: self.handle_get,
            InternalRemoveRequest.COMMAND: self.handle_remove,
            JoinRequest.COMMAND: self.handle_join,
            SetOnlineRequest.COMMAND: self.handle_set_online,
            SetOfflineRequest.COMMAND: self.handle_set_offline,
            PingRequest.COMMAND: self.handle_ping,
            ForwardedRequest.COMMAND: self.handle_forward
        }

    def handle_message(self, message):
        request = Request.from_bytes(message.payload)
        if request.command <= RemoveRequest.COMMAND:
            dest_node = self.node_circle.get_responsible_node_for_key(request.key)
            if dest_node != self.my_node:
                self.forward_request(message, request, dest_node)
                return

        self.handlers[request.command](message, request)

    def reset_pending_requests_for_addr(self, addr):
        for uid, request in self.client.pending_requests.items():
            if(request.dest_addr == addr):
                request.reset()

    def forward_request(self, message, original_request, dest_node):
        request = ForwardedRequest(message.uid, message.sender_addr, original_request)
        self.client.send_request(request, dest_node.get_addr(), self.forward_succeeded, self.forward_failed)

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

        self.handle_message(message)

    def handle_put(self, message, request):
        if self.kvStore.put(request.key, request.value):
            response = SuccessResponse()
        else:
            response = OutOfSpaceResponse()
        self.client.send_response(message, response)

    def handle_get(self, message, request):
        value = self.kvStore.get(request.key)
        if value:
            response = SuccessResponse(value)
        else:
            response = NonexistentKeyResponse()

        self.client.send_response(message, response)

    def handle_remove(self, message, request):
        if self.kvStore.remove(request.key):
            response = SuccessResponse()
        else:
            response = NonexistentKeyResponse()

        self.client.send_response(message, response)

    def handle_shutdown(self, message, request):
        self.client.send_response(message, SuccessResponse())
        sys.exit()

    def handle_join(self, message, request):
        pass

    def handle_set_online(self, message, request):
        self.reset_pending_requests_for_addr(message.sender_addr)
        self.node_circle.set_node_online_with_addr(message.sender_addr, True)
        self.client.send_response(message, SuccessResponse())

    def handle_set_offline(self, message, request):
        self.node_circle.set_node_online_with_addr(request.addr, False)
        self.client.send_response(message, SuccessResponse())

    def handle_ping(self, message, request):
        self.client.send_response(message, SuccessResponse())

    def handle_forward(self, message, request):
        original_request = request.original_request

        uid = request.original_uid
        payload = original_request.get_bytes()
        self.handle_message(Message(uid, payload, request.return_addr))
        self.handle_message(Message(message.uid, payload, message.sender_addr))

    def handle_unrecognized(self, message, resquest):
        self.client.send_response(message, UnrecognizedCommandResponse())
