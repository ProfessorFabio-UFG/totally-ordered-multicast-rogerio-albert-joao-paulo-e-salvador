import pickle
import threading

global_clock = 0
# Adiciona uma trava para proteger o relógio contra acesso concorrente por múltiplas threads.
# Isso evita condições de corrida e garante que os timestamps sejam consistentes.
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

def send(sock, msg_tuple, addr):
    """Cria um timestamp, anexa à mensagem e a envia."""
    timestamp = tick()
    stamped_msg = (*msg_tuple, timestamp)
    try:
        sock.sendto(pickle.dumps(stamped_msg), addr)
    except Exception as e:
        print(f"[ERROR_SEND] Falha ao enviar para {addr}: {e}")
    return timestamp

def receive(sock, bufsize=4096):
    """
    Recebe uma mensagem, desempacota, atualiza o relógio local e, crucialmente,
    retorna o TIMESTAMP ORIGINAL que veio com a mensagem.
    """
    data, addr = sock.recvfrom(bufsize)
    unpacked = pickle.loads(data)
    
    # Separa a carga útil do timestamp recebido na mensagem
    *payload, recv_ts = unpacked
    
    # Atualiza o relógio local com base no timestamp recebido
    update(recv_ts)
    
    # CORREÇÃO CRÍTICA: Retorna o timestamp original (recv_ts),
    # não o novo valor do relógio local. Isso garante que todos os peers
    # identifiquem a mensagem pelo mesmo ID (timestamp, sender_id).
    return tuple(payload), addr, recv_ts
