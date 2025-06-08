from socket import *
from constMP import *
import threading
import random
import time
import pickle
import os
import clock_middleware as cm
from clock_middleware import send, receive

# Field definitions
HANDSHAKE='READY'
DATA='DATA'
PROPOSE='PROPOSE'
FINAL='FINAL'

# Synchronization
handshake_done = threading.Event()

# Peer list (resolved per round)
PEERS = []  # list of IP strings
my_id = None
nMsgs = None

# Hold-back queue: store tuples (timestamp, sender_id, seq, payload_type)
from queue import PriorityQueue
hold_back = PriorityQueue()
# Collect proposals: proposals[seq] = list of proposed timestamps
proposals = {}

# UDP sockets
sendSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))

# TCP socket for start signal
serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', PEER_TCP_PORT))
serverSock.listen(1)

# Utility to write log
def write_log(logList):
    path = os.path.join('/tmp', f'logfile{my_id}.log')
    with open(path, 'w') as lf:
        lf.write(str(logList))
    print(f"Handler: log written to {path}")

class MsgHandler(threading.Thread):
    def __init__(self, sock):
        super().__init__(daemon=True)
        self.sock = sock

    def run(self):
        # 1) Handshake phase
        needed = len(PEERS)
        count = 0
        print('Handler: Waiting for handshakes...')
        while count < needed:
            (msg_type, sender), addr, ts = receive(self.sock)
            if msg_type == HANDSHAKE:
                count += 1
                print(f"--- Handshake {count}/{needed} from Peer{sender}; recv_ts={ts}, clock={cm.global_clock}")
        handshake_done.set()
        print('Handler: All handshakes received. Entering DATA receive loop...')

        # 2) DATA/PROPOSE/FINAL loop
        delivered = []  # list of (sender, seq)
        # track how many STOPs seen
        stopCount = 0
        while True:
            (msg_type, *fields), addr, ts = receive(self.sock)
            if msg_type == DATA:
                sender, seq = fields
                # enqueue with initial timestamp
                hold_back.put((ts, sender, seq))
                # send proposal
                prop_ts = cm.tick()
                for peer in PEERS:
                    send(sendSocket, (PROPOSE, seq, prop_ts, my_id), (peer, PEER_UDP_PORT))
            elif msg_type == PROPOSE:
                seq, prop_ts, proposer = fields
                proposals.setdefault(seq, []).append(prop_ts)
                # when all proposals collected, send final
                if len(proposals[seq]) == len(PEERS):
                    final_ts = max(proposals[seq])
                    cm.tick()
                    for peer in PEERS:
                        send(sendSocket, (FINAL, seq, final_ts), (peer, PEER_UDP_PORT))
            elif msg_type == FINAL:
                seq, final_ts = fields
                # locate and update in hold_back
                temp = []
                while not hold_back.empty():
                    item = hold_back.get()
                    if item[2] == seq:
                        # replace timestamp
                        temp.append((final_ts, item[1], item[2]))
                    else:
                        temp.append(item)
                for it in temp:
                    hold_back.put(it)
                # deliver any ready messages in order
                while not hold_back.empty():
                    ts0, s0, seq0 = hold_back.queue[0]
                    delivered.append((s0, seq0))
                    hold_back.get()
                    print(f"[DELIVER] Msg {seq0} from Peer{s0}, final_ts={ts0}")
            # interpret STOP markers as FINAL for sequence -1
            elif msg_type == DATA and fields[1] == -1:
                stopCount += 1
                if stopCount == len(PEERS):
                    break
        # write and send log
        write_log(delivered)
        client = socket(AF_INET, SOCK_STREAM)
        client.connect((SERVER_ADDR, SERVER_PORT))
        client.send(pickle.dumps(delivered))
        client.close()
        exit(0)

# Wait for start
def waitToStart():
    conn, _ = serverSock.accept()
    data = conn.recv(1024)
    pid, msgs = pickle.loads(data)
    conn.send(pickle.dumps(f'Peer {pid} started.'))
    conn.close()
    return pid, msgs

if __name__ == '__main__':
    # initial registration
    client = socket(AF_INET, SOCK_STREAM)
    client.connect((GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
    client.send(pickle.dumps({'op':'register','ipaddr':gethostname()}))
    client.close()

    # wait for start
    print('Waiting for signal to start...')
    my_id, nMsgs = waitToStart()
    print(f'I am peer {my_id}, will send {nMsgs} messages')

    # get peers and drop self
    peers = getListOfPeers()
    PEERS = [ip for ip in peers if ip != gethostname()]
    print('Peer list:', PEERS)

    # clear handshake event
    handshake_done.clear()

    # start handler
    handler = MsgHandler(recvSocket)
    handler.start() 

    # handshake phase
    for peer in PEERS:
        ts = cm.tick()
        send(sendSocket, (HANDSHAKE, my_id), (peer, PEER_UDP_PORT))
        print(f'Handshake sent to {peer}, ts={ts}')
    # wait
    handshake_done.wait()
    print('Main: Handshakes complete, starting DATA phase')

    # DATA send phase
    for seq in range(nMsgs):
        time.sleep(random.uniform(0.01,0.1))
        ts = cm.tick()
        for peer in PEERS:
            send(sendSocket, (DATA, my_id, seq), (peer, PEER_UDP_PORT))
            print(f'Sent DATA {seq} to {peer}, ts={ts}')

    # send STOP markers
    for peer in PEERS:
        ts = cm.tick()
        send(sendSocket, (DATA, my_id, -1), (peer, PEER_UDP_PORT))
    handler.join()