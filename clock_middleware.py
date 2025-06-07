import pickle

global_clock = 0

def tick():

    global global_clock
    global_clock += 1
    return global_clock

def update(received_ts):
    
    global global_clock
    global_clock = max(global_clock, received_ts) + 1
    return global_clock


def send(sock, msg_tuple, addr):
    
    timestamp = tick()
    stamped = (*msg_tuple, timestamp)
    sock.sendto(pickle.dumps(stamped), addr)
    return timestamp


def receive(sock, bufsize=1024):
    
    data, addr = sock.recvfrom(bufsize)
    unpacked = pickle.loads(data)
    *payload, recv_ts = unpacked
    update(recv_ts)
    return tuple(payload), addr, recv_ts