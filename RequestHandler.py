import sys
from Node import Node
from Response import *
from Request import *

class RequestHandler:
    def __init__(self, sender, kvStore, node_circle):
        self.sender = sender
        self.kvStore = kvStore

        self.handlers = {
            PutRequest.COMMAND: self.handle_put,
            GetRequest.COMMAND: self.handle_get,
            RemoveRequest.COMMAND: self.handle_remove,
            ShutdownRequest.COMMAND: self.handle_shutdown,
            InternalPutRequest.COMMAND: self.handle_put,
            InternalGetRequest.COMMAND: self.handle_get,
            InternalRemoveRequest.COMMAND: self.handle_remove,
            PingRequest.COMMAND: self.handle_ping,
            ForwardedRequest.COMMAND: self.handle_forward
        }

    def get_response(self, request):
        return self.handlers[request.command](request)

    def handle_put(self, request):
        if self.kvStore.put(request.key, request.value):
            response = SuccessResponse()
        else:
            response = OutOfSpaceResponse()

        return response

    def handle_get(self, request):
        value = self.kvStore.get(request.key)
        if value:
            response = SuccessResponse(value)
        else:
            response = NonexistentKeyResponse()

        return response

    def handle_remove(self, request):
        if self.kvStore.remove(request.key):
            response = SuccessResponse()
        else:
            response = NonexistentKeyResponse()

        return response

    def handle_shutdown(self, request):
        return SuccessResponse()

    def handle_ping(self, request):
        return SuccessResponse()

    def handle_forward(self, request):
        original_request = request.original_request

        # uid = request.original_uid
        # payload = original_request.get_bytes()

        # self.sender.received_data.insert(0, ((str(uid.get_bytes()) + str(payload)), request.return_addr))
        # self.sender.send_response(Message(message.uid, payload, message.sender_addr), SuccessResponse())