from socket import *
import pickle
from constMP import *

port = GROUPMNGR_TCP_PORT
membership = []

def serverLoop():
    serverSock = socket(AF_INET, SOCK_STREAM)
    serverSock.bind(('0.0.0.0', port))
    serverSock.listen(N)
    while True:
        (conn, addr) = serverSock.accept()
        msgPack = conn.recv(2048)
        req = pickle.loads(msgPack)
        if req["op"] == "register":
            # Register peer as (ip, port) tuple if not already present
            peer_tuple = (req["ipaddr"], req.get("port", PEER_UDP_PORT))
            if peer_tuple not in membership:
                membership.append(peer_tuple)
            print('Registered peer: ', peer_tuple)
        elif req["op"] == "list":
            # Return only the IPs for compatibility, but can be changed to (ip, port) if needed
            peer_ips = [m[0] for m in membership]
            print('List of peers sent to server: ', peer_ips)
            conn.send(pickle.dumps(peer_ips))
        else:
            print('Unknown operation:', req.get("op"))
            conn.send(pickle.dumps({"error": "Unknown operation"}))
        conn.close()

serverLoop()