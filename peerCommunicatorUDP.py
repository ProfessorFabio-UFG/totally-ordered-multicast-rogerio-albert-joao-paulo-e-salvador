import os
from socket  import *
from constMP import * #-
from queue import PriorityQueue
import threading
import random
import time
import pickle
from requests import get
import clock_middleware as cm
from clock_middleware import send, receive



# Threading event to avoid race conditions on handshake
handshake_done = threading.Event()

# message types
HANDSHAKE='READY'
DATA='DATA'
PROPOSE='PROPOSE'
FINAL='FINAL'

PEERS = []
hold_back = PriorityQueue()
proposals = {}
delivered = []
nMsgs_global = 0  # Para rastrear número de mensagens esperadas
# UDP sockets to send and receive data messages:
# Create send socket
sendSocket = socket(AF_INET, SOCK_DGRAM)
#Create and bind receive socket
recvSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))

# TCP socket to receive start signal from the comparison server:
serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', PEER_TCP_PORT))
serverSock.listen(1)


def get_public_ip():
  ipAddr = get('https://api.ipify.org').content.decode('utf8')
  print('My public IP address is: {}'.format(ipAddr))
  return ipAddr

# Function to register this peer with the group manager
def registerWithGroupManager():
  clientSock = socket(AF_INET, SOCK_STREAM)
  print ('Connecting to group manager: ', (GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  ipAddr = get_public_ip()
  req = {"op":"register", "ipaddr":ipAddr, "port":PEER_UDP_PORT}
  msg = pickle.dumps(req)
  print ('Registering with group manager: ', req)
  clientSock.send(msg)
  clientSock.close()

def getListOfPeers():
  clientSock = socket(AF_INET, SOCK_STREAM)
  print ('Connecting to group manager: ', (GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  req = {"op":"list"}
  msg = pickle.dumps(req)
  print ('Getting list of peers from group manager: ', req)
  clientSock.send(msg)
  msg = clientSock.recv(2048)
  PEERS = pickle.loads(msg)
  print ('Got list of peers: ', PEERS)
  clientSock.close()
  return PEERS


  

class MsgHandler(threading.Thread):
  def __init__(self, sock):
    threading.Thread.__init__(self)
    self.sock = sock

  def run(self):
    print('Handler is ready. Waiting for the handshakes...')
    
    count = 0
    # Wait until handshakes are received from all other processes
    # (to make sure that all processes are synchronized before they start exchanging messages)
    
    while count < len(PEERS):
      payload, addr, recv_timestamp = receive(self.sock)

      msg_type = payload[0]

      if msg_type != HANDSHAKE:
        continue
      count += 1
      print(f"--- Handshake from Peer{payload[1]}; recv_timestamp={recv_timestamp}, clock={cm.global_clock}")
    
    handshake_done.set()
    print('Secondary Thread: Received all handshakes. Entering the loop to receive messages.')

    # here is queue implementation - Algoritmo de Multicast Totalmente Ordenado
    stopCount = 0 
    pending_finals = {}  # Controla quais mensagens estão aguardando FINAL
    
    while stopCount < len(PEERS):
      (msg_type, *fields), addr, recv_timestamp = receive(self.sock)
      
      if msg_type == DATA:
        sender, msg_num = fields
        
        # Processar mensagem de parada
        if msg_num == -1:
          stopCount += 1
          print(f"[STOP] Recebido sinal de parada do Peer {sender}. Total: {stopCount}/{len(PEERS)}")
          if stopCount >= len(PEERS):
            print("[STOP] Todos os peers enviaram sinal de parada. Finalizando...")
            break
          continue
          
        # Processar mensagem normal
        print(f"[RECV] DATA de Peer{sender}: Msg{msg_num}, timestamp={recv_timestamp}")
        hold_back.put((recv_timestamp, sender, msg_num))
        
        # Enviar proposta de timestamp
        proposal_timestamp = cm.tick()
        key = (sender, msg_num)
        pending_finals[key] = False  # Marca que está aguardando FINAL
        
        for peer_id in PEERS:
          send(sendSocket, (PROPOSE, sender, msg_num, proposal_timestamp, myself), (peer_id, PEER_UDP_PORT))
        print(f"[PROPOSE] Enviou proposta {proposal_timestamp} para Msg{msg_num} de Peer{sender}")
      
      elif msg_type == PROPOSE:
        sender, msg_num, proposal_timestamp, proposer = fields
        key = (sender, msg_num)
        proposals.setdefault(key, []).append(proposal_timestamp)
        
        print(f"[PROPOSE] Recebido proposta {proposal_timestamp} de Peer{proposer} para Msg{msg_num} de Peer{sender} ({len(proposals[key])}/{len(PEERS)})")

        # Quando todas as propostas chegaram, enviar timestamp final
        if len(proposals[key]) == len(PEERS):
          final_timestamp = max(proposals[key])
          cm.update(final_timestamp)  # Atualiza relógio com timestamp final
          
          for peer_id in PEERS:
            send(sendSocket, (FINAL, sender, msg_num, final_timestamp), (peer_id, PEER_UDP_PORT))
          print(f"[FINAL] Enviou timestamp final {final_timestamp} para Msg{msg_num} de Peer{sender}")

      elif msg_type == FINAL:
        sender, msg_num, final_timestamp = fields
        key = (sender, msg_num)
        pending_finals[key] = True  # Marca que FINAL foi recebido
        
        print(f"[FINAL] Recebido timestamp final {final_timestamp} para Msg{msg_num} de Peer{sender}")
        
        # Atualizar timestamp na hold_back queue
        temp = []
        while not hold_back.empty():
          item = hold_back.get()
          if item[1] == sender and item[2] == msg_num:
            temp.append((final_timestamp, sender, msg_num))
          else:
            temp.append(item)
        
        for it in temp:
          hold_back.put(it)

        # ALGORITMO DE ENTREGA CORRETO
        # Entrega mensagens em ordem de timestamp final
        can_deliver = True
        while can_deliver and not hold_back.empty():
          can_deliver = False
          
          # Encontra mensagem com menor timestamp
          min_timestamp = float('inf')
          min_msg = None
          
          for item in hold_back.queue:
            timestamp, peer, msg = item
            msg_key = (peer, msg)
            
            # Só considera mensagens que já receberam FINAL
            if msg_key in pending_finals and pending_finals[msg_key]:
              if timestamp < min_timestamp:
                min_timestamp = timestamp
                min_msg = item
          
          # Se encontrou mensagem válida para entrega
          if min_msg is not None:
            timestamp, peer, msg = min_msg
            
            # Remove da hold_back queue
            temp = []
            while not hold_back.empty():
              item = hold_back.get()
              if item != min_msg:
                temp.append(item)
            
            for it in temp:
              hold_back.put(it)
            
            # Entrega a mensagem
            delivered.append((peer, msg))
            msg_key = (peer, msg)
            del pending_finals[msg_key]  # Remove do controle
            
            print(f"[DELIVER] Msg {msg} from Peer{peer}, final_timestamp={timestamp}, total_delivered={len(delivered)}")
            can_deliver = True
      
        
    # Write log file
    print(f"\n========== GERAÇÃO DE LOG - PEER {myself} ==========")
    print(f"[LOG] Peer {myself} finalizou. Total de mensagens entregues: {len(delivered)}")
    print(f"[LOG] Esperado: {len(PEERS)} peers × {nMsgs_global} mensagens = {len(PEERS) * nMsgs_global} mensagens")
    
    if len(delivered) != len(PEERS) * nMsgs_global:
      print(f"[AVISO] Número de mensagens entregues incorreto!")
      print(f"[DEBUG] Mensagens pendentes na hold_back queue: {hold_back.qsize()}")
      if not hold_back.empty():
        print("[DEBUG] Mensagens pendentes:")
        for item in hold_back.queue:
          print(f"  - {item}")
    
    print(f"[LOG] Sequência de entrega (peer, mensagem):")
    for i, (peer, msg) in enumerate(delivered):
      print(f"  {i+1:3d}. Peer{peer} -> Msg{msg}")
    
    path = os.path.join('/tmp', f'logfile{myself}.log')
    with open(path, 'w') as lf:
      lf.write(str(delivered))
    print(f"[LOG] Arquivo salvo em: {path}")
    
    # Send the list of messages to the server (using a TCP socket) for comparison
    print(f"\n========== ENVIANDO LOG PARA O SERVIDOR ==========")
    print(f"[LOG] Conectando ao servidor {SERVER_ADDR}:{SERVER_PORT}...")
    try:
      clientSock = socket(AF_INET, SOCK_STREAM)
      clientSock.connect((SERVER_ADDR, SERVER_PORT))
      msgPack = pickle.dumps(delivered)
      clientSock.send(msgPack)
      print(f"[LOG] ✓ Log enviado com sucesso! {len(delivered)} mensagens enviadas ao servidor")
      print(f"[LOG] Dados enviados: {delivered}")
      clientSock.close()
    except Exception as e:
      print(f"[LOG] ✗ Erro ao enviar log para o servidor: {e}")
    print(f"========== PEER {myself} FINALIZADO ==========\n")

    exit(0)

# Function to wait for start signal from comparison server:
def waitToStart():
  print("[PEER] Aguardando sinal de início do servidor...")
  (conn, addr) = serverSock.accept()
  msgPack = conn.recv(1024)
  msg = pickle.loads(msgPack)
  myself = msg[0]
  nMsgs = msg[1]
  if nMsgs == 0:
    print(f"[PEER] Sinal de terminação recebido! Peer {myself} finalizando...")
    conn.send(pickle.dumps(f'Peer process {myself} terminating.'))
  else:
    print(f"[PEER] Sinal recebido! Peer ID: {myself}, Mensagens: {nMsgs}")
    conn.send(pickle.dumps('Peer process '+str(myself)+' started.'))
  conn.close()
  return (myself,nMsgs)

def reset():
  hold_back.queue.clear()
  proposals.clear()
  delivered.clear()
  handshake_done.clear()
  cm.global_clock = 0  # Reset do relógio de Lamport
  
# From here, code is executed when program starts:
registerWithGroupManager()
while 1:
  reset()
  print('Waiting for signal to start...')
  (myself, nMsgs) = waitToStart()
  nMsgs_global = nMsgs  # Armazena globalmente para uso no log
  print(f"\n========== PEER {myself} INICIANDO ==========")
  print(f'I am up, and my ID is: {str(myself)}')
  print(f'Vou enviar {nMsgs} mensagens para cada peer')

  if nMsgs == 0:
    print(f'[PEER] Peer {myself} terminando por ordem do servidor.')
    print('========== PEER FINALIZADO PELO SERVIDOR ==========')
    exit(0)

  # Wait for other processes to be ready
  # (fully started processes start sending data messages, which the others try to interpret as control messages) 
  time.sleep(5)

  
  PEERS = getListOfPeers()
  my_ip = get_public_ip()  # Usar IP público em vez de hostname
  PEERS = [ip for ip in PEERS if ip != my_ip]
  print(f"[PEER] Meu IP: {my_ip}")
  print(f"[PEER] Lista de outros peers: {PEERS}")
  
  # Create receiving message handler
  msgHandler = MsgHandler(recvSocket)
  msgHandler.start()
  print('Handler started')
  time.sleep(1)


  
  # Send handshakes

  for addrToSend in PEERS:
    timestamp = send(sendSocket, ('READY', myself), (addrToSend, PEER_UDP_PORT))
    print(f"Handshake sent to {addrToSend}, timestamp={timestamp}")


  print('Main Thread: Sent all handshakes. handShakeCount=', len(PEERS))
  handshake_done.wait()
  print('Main Thread: All handshakes received — proceeding to message phase.')
  print(f"\n========== PEER {myself} ENVIANDO MENSAGENS ==========")
  print(f"Enviando {nMsgs} mensagens para {len(PEERS)} peers: {PEERS}")
  
  # Send a sequence of data messages to all other processes 
  for msgNumber in range(nMsgs):
    # Wait some random time between successive messages
    time.sleep(random.randrange(10,100)/1000)
    print(f"\n[SEND] Enviando mensagem {msgNumber + 1}/{nMsgs}...")
    for addrToSend in PEERS:
      timestamp = send(sendSocket, (DATA, myself, msgNumber), (addrToSend, PEER_UDP_PORT))
      print(f"  → Peer {addrToSend}: Msg{msgNumber}, timestamp={timestamp}")

  print(f"\n[SEND] Enviando sinal de STOP para todos os peers...")
  # Tell all processes that I have no more messages to send
  for addrToSend in PEERS:
    stop_timestamp = send(sendSocket, (DATA, myself, -1), (addrToSend, PEER_UDP_PORT))
    print(f"  → Stop para {addrToSend}, timestamp={stop_timestamp}")
  
  print(f"[SEND] Peer {myself} terminou de enviar. Aguardando finalização do handler...")
  msgHandler.join()
  print(f"========== PEER {myself} FINALIZADO ==========\n")
  
