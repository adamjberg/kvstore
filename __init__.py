import socket
import sys
import time
from KVStore import KVStore
from Node import Node
from Response import *
from Request import *
from UDPClient import UDPClient

def init_nodes_from_file():
    with open("hosts.txt") as f:
        lines = [x.strip('\n') for x in f.readlines()]
    for line in lines:
        host, port, location = line.split(":")
        nodes.append(Node(host, port, location))

def init_client():
    global client;
    nodes_for_my_ip = []
    for node in nodes:
        ip = node.ip
        if does_ip_match_mine(ip):
            nodes_for_my_ip.append(node)

    for node in nodes_for_my_ip:
        try:
            client = UDPClient(node.port)
            print "Connected on port: " + str(node.port)
            return True
        except:
            pass
    return False

def does_ip_match_mine(ip):
    if ip.startswith("127.") or ip.startswith("localhost"):
        return True
    elif ip == socket.gethostbyname(socket.gethostname()):
        return True
    return False

def handle_put_request(request):
    if kvStore.put(request.key, request.value):
        response = SuccessResponse()
    else:
        response = OutOfSpaceResponse()
    client.send_response(message, response)
    return None

def handle_get_request(request):
    value = kvStore.get(request.key)
    if value:
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
    client.send_response(message, SuccessResponse())
    sys.exit()

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
    nodes = []
    client = None
    init_nodes_from_file()
    if init_client() is False:
        print "Failed to bind to a port."
        sys.exit()


    kvStore = KVStore()
    while True:
        message = client.receive()
        if message:
            handle_message(message)