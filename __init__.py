import time
from UDPClient import UDPClient
from Response import *
from Request import *
from KVStore import KVStore

def handle_put_request(request):
    if kvStore.put(request.key, request.value):
        response = SuccessResponse()
    else:
        response = OutOfSpaceResponse()
    client.send_response(message, response)
    return None

def handle_get_request(request):
    if kvStore.get(request.key):
        response = SuccessResponse(value)
    else:
        response = NonexistentKeyResponse()

    client.send_response(message, response)

def handle_remove_request(request):    
    if kvStore.remove(request.key):
        response = SuccessResponse()
    else:
        response = NonexistentKeyResponse()

    client.send_response(message, response)

def handle_shutdown_request(request):
    print "HANDLE SHUTDOWN"
    return None

def handle_message(message):
    request_handlers = {
        Request.PUT: handle_put_request,
        Request.GET: handle_get_request,
        Request.REMOVE: handle_remove_request,
        Request.SHUTDOWN: handle_shutdown_request
    }
    request = Request.from_bytes(message.payload)
    request_handlers[request.command](request)

if __name__ == "__main__":
    client = UDPClient(12000)
    kvStore = KVStore()
    while True:
        message = client.receive()
        if message:
            handle_message(message)
