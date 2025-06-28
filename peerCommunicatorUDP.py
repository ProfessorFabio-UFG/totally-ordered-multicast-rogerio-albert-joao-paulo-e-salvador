import os
from socket  import *
from constMP import *
from queue import PriorityQueue
import threading
import time
import pickle
from requests import get
import clock_middleware as cm
from clock_middleware import stamp_message, send_stamped, receive
from script import SCRIPT, TOTAL_MESSAGES_IN_SCRIPT

# Evento para sincronizar o início
handshake_done = threading.Event()
# Tipos de mensagem
HANDSHAKE = 'READY'
DATA = 'DATA'
ACK = 'ACK'

# --- Variáveis Globais ---
PEERS = []
hold_back = PriorityQueue()
delivered = []
lock = threading.Lock() 
my_ip = None
myself = -1 

# --- Sockets ---
sendSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))
serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', PEER_TCP_PORT))
serverSock.listen(1)

# --- Funções de Rede ---
def get_public_ip():
  if GROUPMNGR_ADDR == '127.0.0.1' or GROUPMNGR_ADDR == 'localhost':
      return '127.0.0.1'
  try:
      ipAddr = get('https://api.ipify.org').content.decode('utf8')
      return ipAddr
  except Exception as e:
      print(f"[ERROR] Não foi possível obter o IP público: {e}. Usando 127.0.0.1")
      return '127.0.0.1'

def registerWithGroupManager():
  try:
    with socket(AF_INET, SOCK_STREAM) as clientSock:
      clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
      ipAddr = get_public_ip()
      req = {"op":"register", "ipaddr":ipAddr, "port":PEER_UDP_PORT}
      msg = pickle.dumps(req)
      clientSock.send(msg)
      print(f"Registrado com o gerenciador de grupo: {req}")
  except Exception as e:
      print(f"ERRO FATAL ao registrar: {e}")
      os._exit(1)

def getListOfPeers():
  global PEERS
  try:
    with socket(AF_INET, SOCK_STREAM) as clientSock:
      clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
      req = {"op":"list"}
      msg = pickle.dumps(req)
      clientSock.send(msg)
      msg_recv = clientSock.recv(2048)
      # CORREÇÃO CRÍTICA: A lista de peers deve sempre conter todos os IPs.
      PEERS = pickle.loads(msg_recv)
      return PEERS
  except Exception as e:
      print(f"ERRO FATAL ao obter lista de peers: {e}")
      os._exit(1)

# --- Lógica do Handler de Mensagens ---
class MsgHandler(threading.Thread):
    def __init__(self, sock):
        threading.Thread.__init__(self)
        self.sock = sock

    def run(self):
        print(f'[HANDLER {myself}] Pronto para processar todas as mensagens.')
        
        acks = {}
        received_handshakes = set()

        while len(delivered) < TOTAL_MESSAGES_IN_SCRIPT:
            try:
                (msg_type, *fields), addr, recv_timestamp = receive(self.sock)

                if msg_type == HANDSHAKE:
                    sender_id = fields[0]
                    if sender_id not in received_handshakes:
                        received_handshakes.add(sender_id)
                        print(f"[HANDLER {myself}] ✓ Handshake recebido do Peer {sender_id}. Total: {len(received_handshakes)}/{len(PEERS)}")
                    
                    if len(received_handshakes) == len(PEERS) and not handshake_done.is_set():
                        print(f'[HANDLER {myself}] Todos os handshakes recebidos. Liberando a thread principal.')
                        handshake_done.set()
                
                elif msg_type == DATA:
                    sender, msg_content = fields
                    msg_key = (recv_timestamp, sender)
                    if msg_key not in acks:
                        hold_back.put((recv_timestamp, sender, msg_content))
                        acks[msg_key] = {myself}
                        stamped_ack = cm.stamp_message((ACK, myself, msg_key))
                        for peer_ip in PEERS: # Envia ACK para todos, incluindo a si mesmo
                            cm.send_stamped(sendSocket, stamped_ack, (peer_ip, PEER_UDP_PORT))
                
                elif msg_type == ACK:
                    sender_id, original_msg_key = fields
                    if original_msg_key in acks:
                        acks[original_msg_key].add(sender_id)

            except Exception:
                break

            can_deliver = True
            while can_deliver:
                can_deliver = False
                if not hold_back.empty():
                    top_ts, top_sender, top_content = hold_back.queue[0]
                    top_key = (top_ts, top_sender)
                    
                    if top_key in acks and len(acks.get(top_key, set())) == len(PEERS):
                        hold_back.get()
                        
                        with lock:
                            if (top_sender, top_content) not in delivered:
                                delivered.append((top_sender, top_content))
                                print(f"->> [DELIVER {myself}] Entregue: '{top_content}' (de Peer {top_sender}). Total: {len(delivered)} <<--")
                        
                        del acks[top_key]
                        can_deliver = True

        print(f"[HANDLER {myself}] Todas as mensagens do roteiro foram entregues. Encerrando.")
        
        print(f"\n[LOG] Enviando log final para o servidor...")
        try:
            with socket(AF_INET, SOCK_STREAM) as clientSock:
                clientSock.connect((SERVER_ADDR, SERVER_PORT))
                with lock:
                    msgPack = pickle.dumps(delivered)
                clientSock.sendall(msgPack)
                print("[LOG] ✓ Log enviado com sucesso!")
        except Exception as e:
            print(f"[LOG] ✗ Erro ao enviar log para o servidor: {e}")

# --- Funções de Controle ---
def waitToStart():
    print("[PEER] Aguardando sinal de início do servidor...")
    (conn, addr) = serverSock.accept()
    msgPack = conn.recv(1024)
    msg = pickle.loads(msgPack)
    peer_id, mode = msg[0], msg[1]
    
    if mode == 0:
        conn.send(pickle.dumps(f'Peer process {peer_id} terminating.'))
    else:
        conn.send(pickle.dumps('Peer process '+str(peer_id)+' started for script mode.'))
    conn.close()
    return (peer_id, mode)

def reset():
    hold_back.queue.clear()
    delivered.clear()
    handshake_done.clear()
    cm.global_clock = 0

def main_script_logic():
    global myself
    my_script_part = SCRIPT.get(myself, [])
    script_index = 0
    print(f"\n========== PEER {myself} EXECUTANDO ROTEIRO ==========")
    while script_index < len(my_script_part):
        if len(delivered) >= TOTAL_MESSAGES_IN_SCRIPT:
            break
            
        message_to_send, wait_for_log_size = my_script_part[script_index]
        
        current_log_size = 0
        with lock:
            current_log_size = len(delivered)
        
        if current_log_size >= wait_for_log_size:
            print(f"\n[SEND] Minha deixa! (log tem {current_log_size} msgs). Enviando: '{message_to_send}'")
            stamped_data_msg = cm.stamp_message((DATA, myself, message_to_send))
            for addrToSend in PEERS: # Envia para todos na lista, incluindo a si mesmo.
                cm.send_stamped(sendSocket, stamped_data_msg, (addrToSend, PEER_UDP_PORT))
            
            script_index += 1
        time.sleep(0.2)
    print(f"[PEER {myself}] Roteiro finalizado. Aguardando a entrega de todas as mensagens...")

# ===================================================================
# INÍCIO DA EXECUÇÃO PRINCIPAL
# ===================================================================
registerWithGroupManager()
while True:
    reset()
    (peer_id, mode) = waitToStart()
    myself = peer_id

    if mode == 0:
        print(f'[PEER {myself}] Terminando por ordem do servidor.')
        break

    if mode == -1:
        PEERS = getListOfPeers() # A lista de PEERS agora é usada consistentemente
        my_ip = get_public_ip()
        print(f"[PEER] Meu ID: {myself}, Meu IP: {my_ip}, Lista de Peers: {PEERS}")

        msgHandler = MsgHandler(recvSocket)
        msgHandler.daemon = True
        msgHandler.start()
        
        time.sleep(2)

        print("[PEER] Enviando handshakes para todos...")
        stamped_handshake = cm.stamp_message((HANDSHAKE, myself))
        for peer_ip in PEERS:
            cm.send_stamped(sendSocket, stamped_handshake, (peer_ip, PEER_UDP_PORT))

        print("[PEER] Aguardando todos os peers ficarem prontos...")
        handshake_done.wait(timeout=15)
        
        if not handshake_done.is_set():
            print("[ERRO] Timeout! Nem todos os peers enviaram handshake. Abortando.")
            continue

        print("[PEER] Sincronizado! Iniciando lógica do roteiro.")
        main_script_logic()
        
        msgHandler.join(timeout=60)
        if msgHandler.is_alive():
            print("[AVISO] Handler não terminou a tempo. Pode haver mensagens presas.")

        print(f"========== PEER {myself} FINALIZADO ==========\n")
    
print('========== PROCESSO PEER ENCERRADO ==========')
