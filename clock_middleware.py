import pickle
import threading

global_clock = 0
# Trava para garantir que o relógio seja acessado de forma segura por múltiplas threads
clock_lock = threading.Lock()

def tick():
    global global_clock
    with clock_lock:
        global_clock += 1
        return global_clock

def update(received_ts):
    global global_clock
    with clock_lock:
        global_clock = max(global_clock, received_ts) + 1
        return global_clock

def stamp_message(msg_tuple):
    timestamp = tick()
    return (*msg_tuple, timestamp)

def send_stamped(sock, stamped_msg, addr):
    try:
        sock.sendto(pickle.dumps(stamped_msg), addr)
        print(f"[SEND] Para {addr}: {stamped_msg[0]} - timestamp {stamped_msg[-1]}")
    except Exception as e:
        print(f"[ERROR_SEND] Falha ao enviar para {addr}: {e}")

def receive(sock, bufsize=4096):
    data, addr = sock.recvfrom(bufsize)
    unpacked = pickle.loads(data)
    *payload, recv_ts = unpacked
    update(recv_ts)
    return tuple(payload), addr, recv_ts
