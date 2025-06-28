import pickle
import threading

global_clock = 0
# Adiciona uma trava para proteger o relógio contra acesso concorrente
clock_lock = threading.Lock()

def tick():
    """Incrementa o relógio de forma atômica."""
    global global_clock
    with clock_lock:
        global_clock += 1
        return global_clock

def update(received_ts):
    """Atualiza o relógio de forma atômica."""
    global global_clock
    with clock_lock:
        global_clock = max(global_clock, received_ts) + 1
        return global_clock


def send(sock, msg_tuple, addr):
    """Cria um timestamp, anexa à mensagem e a envia."""
    # O timestamp é criado antes de ser anexado
    timestamp = tick()
    stamped_msg = (*msg_tuple, timestamp)
    try:
        sock.sendto(pickle.dumps(stamped_msg), addr)
    except Exception as e:
        print(f"[ERROR_SEND] Falha ao enviar para {addr}: {e}")
    return timestamp


def receive(sock, bufsize=2048):
    """Recebe uma mensagem, desempacota e atualiza o relógio."""
    data, addr = sock.recvfrom(bufsize)
    unpacked = pickle.loads(data)
    # Separa a carga útil do timestamp recebido
    *payload, recv_ts = unpacked
    # Atualiza o relógio local com base no timestamp recebido
    update(recv_ts)
    return tuple(payload), addr, recv_ts
