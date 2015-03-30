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
from HandleJoinThread import *
from MonitorNodeThread import *

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
            JoinSuccessRequest.COMMAND: self.handle_join_success,
            SetOnlineRequest.COMMAND: self.handle_set_online,
            SetOfflineRequest.COMMAND: self.handle_set_offline,
            PingRequest.COMMAND: self.handle_ping,
            ForwardedRequest.COMMAND: self.handle_forward
        }

    def handle_message(self, message):
        request = Request.from_bytes(message.payload)
        if request.command <= RemoveRequest.COMMAND:
            dest_node = self.node_circle.get_master_node_for_key(request.key)
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
        self.client.send_request(request, dest_node.get_addr())

    def handle_put(self, message, request):
        if self.kvStore.put(request.key, request.value):
            response = SuccessResponse()
        else:
            response = OutOfSpaceResponse()

        self.client.send_response(message, response)

        if request.command == PutRequest.COMMAND:
            self.replicate_request(InternalPutRequest(request.key, request.value))

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
        
        if request.command == RemoveRequest.COMMAND:
            self.replicate_request(InternalRemoveRequest(request.key))

    # Pass on the requests, don't worry about success or failure
    def replicate_request(self, request):
        for node in self.node_circle.get_nodes_for_key(request.key):
            if node != self.node_circle.my_node:
                self.client.send_request(request, node.get_addr())

    def handle_shutdown(self, message, request):
        self.client.send_response(message, SuccessResponse())
        sys.exit()

    def handle_join(self, message, request):
        self.client.send_response(message, SuccessResponse())

        node = self.node_circle.get_node_with_addr(message.sender_addr)

        handle_join_thread = HandleJoinThread(self.client, self.node_circle, node, self.kvStore)
        handle_join_thread.start()

    def handle_join_success(self, message, request):
        self.client.send_response(message, SuccessResponse())
        self.send_set_online_request()
        monitor_node_thread = MonitorNodeThread(self.client, self.node_circle, self.kvStore)
        monitor_node_thread.start()

    def send_set_online_request(self):
        request = SetOnlineRequest()
        for node in self.node_circle.nodes:
            self.client.send_request(request, node.get_addr(), self.set_online_success, self.set_online_failed)

    def set_online_success(self, message):
        self.node_circle.set_node_online_with_addr(message.sender_addr, True)

    def set_online_failed(self, request):
        self.node_circle.set_node_online_with_addr(request.dest_addr, False)

    def handle_set_online(self, message, request):
        self.reset_pending_requests_for_addr(message.sender_addr)
        self.node_circle.set_node_online_with_addr(message.sender_addr, True)
        self.client.send_response(message, SuccessResponse())

    def handle_set_offline(self, message, request):
        down_node = self.node_circle.get_node_with_addr(request.addr)

        replica_nodes = self.node_circle.get_replica_nodes()

        down_node.online = False
        node_to_replicate_to = self.node_circle.get_last_replica()

        # If the down node was a replica node of this node, replicate to your
        # furthest replica node
        if down_node in replica_nodes and node_to_replicate_to:
            for key, value in self.kvStore.kv_dict.items():
                if self.node_circle.get_master_node_for_key(key) == self.node_circle.my_node:
                    # For now ignore the response
                    self.client.send_request(InternalPutRequest(key,value), node_to_replicate_to.get_addr())

        self.client.send_response(message, SuccessResponse())        

    def handle_ping(self, message, request):
        self.client.send_response(message, SuccessResponse())

    def handle_forward(self, message, request):
        original_request = request.original_request

        uid = request.original_uid
        payload = original_request.get_bytes()
        self.handle_message(Message(uid, payload, request.return_addr))
        self.client.send_response(Message(message.uid, payload, message.sender_addr), SuccessResponse())

    def handle_unrecognized(self, message, resquest):
        self.client.send_response(message, UnrecognizedCommandResponse())
