import hashlib
import socket
import sys
from KVStore import KVStore
from Message import Message
from Node import Node
from Response import *
from Request import *
from UDPClient import UDPClient
from UID import UID

def init_nodes_from_file():
    with open("hosts.txt") as f:
        lines = [x.strip('\n') for x in f.readlines()]
    for line in lines:
        host, port, location = line.split(":")
        nodes.append(Node(host, port, location))

def init_client():
    global client;
    global my_node;
    nodes_for_my_ip = []
    for node in nodes:
        ip = node.ip
        if does_ip_match_mine(ip):
            nodes_for_my_ip.append(node)

    for node in nodes_for_my_ip:
        try:
            client = UDPClient(node.port)
            my_node = node
            print "Connected on port: " + str(node.port)
            return True
        except socket.error:
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
    try:
        request = Request.from_bytes(message.payload)
    except:
        print "PARSE ERROR " + str(message.get_bytes())
        return

    if request.command == Request.SHUTDOWN:
        handle_shutdown_request(request)

    dest_node = get_responsible_node_for_key(request.key)

    if dest_node == my_node:
        if request.command == Request.PUT:
            handle_put_request(request)
        elif request.command == Request.GET:
            handle_get_request(request)
        elif request.command == Request.REMOVE:
            handle_remove_request(request)
        else:
            client.send_response(message, UnrecognizedCommandResponse())
    else:
        forward_request(message, dest_node)

def forward_request(message, dest_node):
    client.send_request(message.get_bytes(), (dest_node.ip, dest_node.port), forward_succeeded, forward_failed)

def forward_succeeded():
    print "SUCCESS"

def forward_failed(request):
    for node in nodes:
        if node.ip == request.dest_addr[0] and node.port == request.dest_addr[1]:
            node.online = False

    uid = UID.from_bytes(request.payload)
    payload = request.payload[UID.LENGTH * 2:]
    message = Message(uid, payload, request.source_addr)
    handle_message(message)

def get_responsible_node_for_key(key):
    dest_node = nodes[-1]
    location = get_location_for_key(key)
    for node in nodes:
        if node.online == False:
            continue

        if location >= node.location:
            dest_node = node

    return dest_node

def get_location_for_key(key):
    return struct.unpack('B', hashlib.sha256(key).digest()[0])[0]

if __name__ == "__main__":
    my_node = None
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