import pickle
import threading

global_clock = 0
# Trava para garantir que o relógio seja acessado de forma segura por múltiplas threads
clock_lock = threading.Lock()

def tick():
    """Incrementa o relógio de forma atômica e segura."""
    global global_clock
    with clock_lock:
        global_clock += 1
        return global_clock

def update(received_ts):
    """Atualiza o relógio de forma atômica e segura."""
    global global_clock
    with clock_lock:
        global_clock = max(global_clock, received_ts) + 1
        return global_clock

def stamp_message(msg_tuple):
    """
    Apenas adiciona um timestamp a uma tupla de mensagem.
    Não envia a mensagem. Isso garante que a mensagem seja criada uma única vez.
    """
    timestamp = tick()
    return (*msg_tuple, timestamp)

def send_stamped(sock, stamped_msg, addr):
    """
    Apenas envia uma mensagem que JÁ FOI CARIMBADA.
    Isso será usado no loop para enviar a mesma mensagem para todos.
    """
    try:
        sock.sendto(pickle.dumps(stamped_msg), addr)
    except Exception as e:
        print(f"[ERROR_SEND] Falha ao enviar para {addr}: {e}")

def receive(sock, bufsize=4096):
    """
    Recebe uma mensagem, desempacota, atualiza o relógio local e, crucialmente,
    retorna o TIMESTAMP ORIGINAL que veio com a mensagem.
    """
    data, addr = sock.recvfrom(bufsize)
    unpacked = pickle.loads(data)
    *payload, recv_ts = unpacked
    update(recv_ts)
    return tuple(payload), addr, recv_ts
