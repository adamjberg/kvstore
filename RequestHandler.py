import sys
import time
from Node import Node
from Response import *
from Request import *
from HandleJoinThread import *

class RequestHandler:
    def __init__(self, sender, kv_store, node_circle):
        self.sender = sender
        self.kv_store = kv_store
        self.node_circle = node_circle

        self.handlers = {
            PutRequest.COMMAND: self.handle_put,
            GetRequest.COMMAND: self.handle_get,
            RemoveRequest.COMMAND: self.handle_remove,
            ShutdownRequest.COMMAND: self.handle_shutdown,
            InternalPutRequest.COMMAND: self.handle_put,
            InternalGetRequest.COMMAND: self.handle_get,
            InternalRemoveRequest.COMMAND: self.handle_remove,
            PingRequest.COMMAND: self.handle_success,
            ForwardedRequest.COMMAND: self.handle_forward,
            JoinRequest.COMMAND: self.handle_join,
            DebugInfoRequest.COMMAND: self.handle_debug_info
        }

    def handle_request(self, uid, request, sender_address):
        self.handlers[request.command](uid, request, sender_address)

    def handle_put(self, uid, request, sender_address):
        if self.kv_store.put(self.node_circle.get_location_for_key(request.key), request.key, request.value):
            response = SuccessResponse()
            
            if request.command == PutRequest.COMMAND:
                self.send_to_replicas(InternalPutRequest(request.key, request.value))

        else:
            response = OutOfSpaceResponse()

        self.reply(uid, response, sender_address)


    def handle_get(self, uid, request, sender_address):
        value = self.kv_store.get(self.node_circle.get_location_for_key(request.key), request.key)
        if value:
            response = SuccessResponse(value)
        else:
            response = NonexistentKeyResponse()

        self.reply(uid, response, sender_address)

    def handle_remove(self, uid, request, sender_address):
        if self.kv_store.remove(self.node_circle.get_location_for_key(request.key), request.key):
            response = SuccessResponse()
            
            if request.command == PutRequest.COMMAND:
                self.send_to_replicas(InternalRemoveRequest(request.key, request.value))

        else:
            response = NonexistentKeyResponse()

        self.reply(uid, response, sender_address)

    def handle_shutdown(self, uid, request, sender_address):
        self.reply(SuccessResponse(), sender_address)
        sys.exit()

    def handle_forward(self, uid, request, sender_address):
        self.handle_success(uid, request, sender_address)

        original_request = request.original_request

        uid = request.original_uid
        payload = original_request.get_bytes()

        if self.sender.check_cached_responses(uid, request.return_addr):
            return

        forwarded_request = Request.from_bytes(payload)
        self.handle_request(uid, forwarded_request, request.return_addr)

    def handle_join(self, uid, request, sender_address):
        self.handle_success(uid, request, sender_address)

        new_node = self.node_circle.get_node_with_address(sender_address)
        handle_join_thread = HandleJoinThread(self.sender, self.node_circle, new_node, self.kv_store)
        handle_join_thread.start()

    def handle_debug_info(self, uid, request, sender_address):
        data = ""
        for node in self.node_circle.nodes:
            data += node.hostname + " " + str(node.online) + " " + str(node.average_rtt) + "\n"

        self.reply(uid, SuccessResponse(str(data)), sender_address)

    def handle_success(self, uid, request, sender_address):
        self.reply(uid, SuccessResponse(), sender_address)

    def reply(self, uid, response, sender_address):
        self.sender.send_response(uid, response, sender_address)

    def send_to_replicas(self, request):
        for node in self.node_circle.get_replica_nodes_for_key(request.key):
            self.sender.send_request(request, node)