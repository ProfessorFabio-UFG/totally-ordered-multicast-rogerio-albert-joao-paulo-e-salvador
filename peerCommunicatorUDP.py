import os
from socket  import *
from constMP import *
from queue import PriorityQueue
import threading
import time
import pickle
from requests import get
import clock_middleware as cm
from clock_middleware import send, receive
# Importa o roteiro
from script import SCRIPT, TOTAL_MESSAGES_IN_SCRIPT

# Evento para sincronizar o início, como no seu código original
handshake_done = threading.Event()
# Tipos de mensagem
HANDSHAKE = 'READY'
DATA = 'DATA'
ACK = 'ACK'

PEERS = []
hold_back = PriorityQueue()
delivered = []
my_ip = None
myself = -1 # ID deste peer

# --- Sockets (sem alterações) ---
sendSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))
serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', PEER_TCP_PORT))
serverSock.listen(1)

# --- Funções de Rede (sem alterações) ---
def get_public_ip():
  if GROUPMNGR_ADDR == '127.0.0.1' or GROUPMNGR_ADDR == 'localhost':
      return '127.0.0.1'
  ipAddr = get('https://api.ipify.org').content.decode('utf8')
  print('Meu endereço IP público é: {}'.format(ipAddr))
  return ipAddr

def registerWithGroupManager():
  clientSock = socket(AF_INET, SOCK_STREAM)
  print ('Conectando ao gerenciador de grupo: ', (GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  ipAddr = get_public_ip()
  req = {"op":"register", "ipaddr":ipAddr, "port":PEER_UDP_PORT}
  msg = pickle.dumps(req)
  print ('Registrando com o gerenciador de grupo: ', req)
  clientSock.send(msg)
  clientSock.close()

def getListOfPeers():
  global PEERS
  clientSock = socket(AF_INET, SOCK_STREAM)
  clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  req = {"op":"list"}
  msg = pickle.dumps(req)
  clientSock.send(msg)
  msg_recv = clientSock.recv(2048)
  PEERS = pickle.loads(msg_recv)
  clientSock.close()
  return PEERS

# --- Lógica do Handler de Mensagens (com handshake reintroduzido) ---
class MsgHandler(threading.Thread):
    def __init__(self, sock):
        threading.Thread.__init__(self)
        self.sock = sock

    def run(self):
        print('[HANDLER] Pronto. Aguardando handshakes...')
        
        # --- Fase de Handshake ---
        handshake_count = 0
        # Espera handshake de todos os outros peers.
        while handshake_count < len(PEERS):
            try:
                (msg_type, *fields), addr, recv_timestamp = receive(self.sock)
                if msg_type == HANDSHAKE:
                    sender_id = fields[0]
                    handshake_count += 1
                    print(f"[HANDLER] ✓ Handshake recebido do Peer {sender_id}. Total: {handshake_count}/{len(PEERS)}")
            except Exception:
                continue
        
        # Libera a thread principal para começar a enviar
        handshake_done.set()
        print('[HANDLER] Todos os handshakes recebidos. Processando mensagens...')

        # --- Fase de Mensagens ---
        acks = {} 
        
        while len(delivered) < TOTAL_MESSAGES_IN_SCRIPT:
            try:
                (msg_type, *fields), addr, recv_timestamp = receive(self.sock)
                
                # Ignora handshakes tardios
                if msg_type == HANDSHAKE:
                    continue

                if msg_type == DATA:
                    sender, msg_content = fields
                    msg_key = (recv_timestamp, sender)
                    if msg_key not in acks: # Evita processar duplicatas de UDP
                        hold_back.put((recv_timestamp, sender, msg_content))
                        acks[msg_key] = {myself} # Adiciona o próprio ACK
                        # Envia ACK para todos
                        for peer_ip in PEERS:
                            send(sendSocket, (ACK, myself, msg_key), (peer_ip, PEER_UDP_PORT))
                
                elif msg_type == ACK:
                    sender_id, original_msg_key = fields
                    if original_msg_key in acks:
                        acks[original_msg_key].add(sender_id)

                # Loop de entrega
                delivered_something = True
                while delivered_something:
                    delivered_something = False
                    if not hold_back.empty():
                        top_ts, top_sender, top_content = hold_back.queue[0]
                        top_key = (top_ts, top_sender)
                        
                        if top_key in acks and len(acks[top_key]) == len(PEERS):
                            _, delivered_sender, delivered_content = hold_back.get()
                            if (delivered_sender, delivered_content) not in delivered:
                                delivered.append((delivered_sender, delivered_content))
                                print(f"[DELIVER] Entregue: '{delivered_content}' (de Peer {delivered_sender}). Total: {len(delivered)}")
                                delivered_something = True
                            del acks[top_key]

            except Exception as e:
                # print(f"Erro no loop do handler: {e}")
                continue
        
        print("[HANDLER] Todas as mensagens do roteiro foram entregues. Encerrando.")
        
        # Envia log para o servidor
        print(f"\n[LOG] Enviando log final para o servidor: {len(delivered)} mensagens.")
        try:
            with socket(AF_INET, SOCK_STREAM) as clientSock:
                clientSock.connect((SERVER_ADDR, SERVER_PORT))
                msgPack = pickle.dumps(delivered)
                clientSock.sendall(msgPack) # Usar sendall para garantir envio completo
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
    print(f"[PEER] Sinal de terminação recebido! Peer {peer_id} finalizando...")
    conn.send(pickle.dumps(f'Peer process {peer_id} terminating.'))
  else:
    print(f"[PEER] Sinal recebido! Peer ID: {peer_id}, Modo: Roteiro")
    conn.send(pickle.dumps('Peer process '+str(peer_id)+' started for script mode.'))
  conn.close()
  return (peer_id, mode)

def reset():
  hold_back.queue.clear()
  delivered.clear()
  handshake_done.clear() # Limpa o evento para a próxima rodada
  cm.global_clock = 0

def main_script_logic():
    global myself
    my_script_part = SCRIPT.get(myself, [])
    script_index = 0
    print(f"\n========== PEER {myself} EXECUTANDO ROTEIRO ==========")
    while script_index < len(my_script_part):
        message_to_send, wait_for_log_size = my_script_part[script_index]
        if len(delivered) >= wait_for_log_size:
            print(f"\n[SEND] Minha deixa! (log tem {len(delivered)} msgs). Enviando: '{message_to_send}'")
            for addrToSend in PEERS:
                send(sendSocket, (DATA, myself, message_to_send), (addrToSend, PEER_UDP_PORT))
            script_index += 1
        time.sleep(0.2)
    print(f"[PEER {myself}] Roteiro finalizado. Aguardando a entrega de todas as mensagens...")

# ===================================================================
# INÍCIO DA EXECUÇÃO PRINCIPAL
# ===================================================================
registerWithGroupManager()
while True:
    reset()
    print('Aguardando sinal para iniciar...')
    (peer_id, mode) = waitToStart()
    myself = peer_id

    if mode == 0:
        print(f'[PEER] Peer {myself} terminando por ordem do servidor.')
        break

    if mode == -1: # Modo Roteiro
        PEERS = getListOfPeers()
        my_ip = get_public_ip()
        print(f"[PEER] Meu ID: {myself}, Meu IP: {my_ip}")
        print(f"[PEER] Lista completa de peers: {PEERS}")

        msgHandler = MsgHandler(recvSocket)
        msgHandler.daemon = True
        msgHandler.start()
        print('Handler de mensagens iniciado.')
        
        time.sleep(2) # Dar tempo para todos os handlers iniciarem

        # --- Envio de Handshakes ---
        print("[PEER] Enviando handshakes para todos...")
        for peer_ip in PEERS:
            # Não precisa enviar para si mesmo, mas não atrapalha
            send(sendSocket, (HANDSHAKE, myself), (peer_ip, PEER_UDP_PORT))

        # --- Aguarda Sincronização ---
        print("[PEER] Aguardando todos os peers ficarem prontos...")
        handshake_done.wait(timeout=15) # Espera o evento ser setado pelo handler
        
        if not handshake_done.is_set():
            print("[ERRO] Timeout! Nem todos os peers enviaram handshake. Abortando.")
            continue

        print("[PEER] Sincronizado! Iniciando lógica do roteiro.")
        main_script_logic()
        
        msgHandler.join(timeout=60)
        if msgHandler.is_alive():
            print("[AVISO] Handler não terminou. Pode haver mensagens presas.")

        print(f"========== PEER {myself} FINALIZADO ==========\n")
    
print('========== PROCESSO PEER ENCERRADO ==========')

