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

#handShakes = [] # not used; only if we need to check whose handshake is missing

# Counter to make sure we have received handshakes from all other processes

handshake_done = threading.Event()


HANDSHAKE='READY'
DATA='DATA'
PROPOSE='PROPOSE'
FINAL='FINAL'
PEERS = []

hold_back = PriorityQueue()
proposals = {}
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

def write_log(logList):
    path = os.path.join('/tmp', f'logfile{myself}.log')
    with open(path, 'w') as lf:
        lf.write(str(logList))

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
      (msg_type, peer_id), addr, recv_timestamp = receive(self.sock)

      if msg_type != HANDSHAKE:
        continue
      count += 1
      print(f"--- Handshake from Peer{peer_id}; recv_timestamp={recv_timestamp}, clock={cm.global_clock}")
    
    handshake_done.set()
    print('Secondary Thread: Received all handshakes. Entering the loop to receive messages.')

    delivered = []
    stopCount=0 
    while True:
      (msg_type, *fields), addr, recv_timestamp = receive(self.sock)
      
      if msg_type == DATA:
        sender, msg_num = fields
        hold_back.put((recv_timestamp, sender, msg_num))

        proposal_timestamp = cm.tick()
        for peer_id in PEERS:
          send(sendSocket, (PROPOSE, msg_num, proposal_timestamp, myself), (peer_id, PEER_UDP_PORT))
      
      elif msg_type == PROPOSE:
        msg_num, proposal_timestamp, proposer = fields
        proposals.setdefault(msg_num, []).append(proposal_timestamp)

        if len(proposals[msg_num]) == len(PEERS):
          final_timestamp = max(proposals[msg_num])
          cm.tick()
          for peer_id in PEERS:
            send(sendSocket, (FINAL, msg_num, final_timestamp), (peer_id, PEER_UDP_PORT))

      elif msg_type == FINAL:
        msg_num, final_timestamp = fields

        temp = []
        while not hold_back.empty():
          item = hold_back.get()
          if item[2] == msg_num:
            temp.append((final_timestamp, item[1], item[2]))
          else:
            temp.append(item)
        
        for it in temp:
          hold_back.put(it)

        while not hold_back.empty():
          timestamp0, peer, msg0 = hold_back.queue[0]
          delivered.append((peer, msg0))
          hold_back.get()
          print(f"[DELIVER] Msg {msg0} from Peer{peer}, clock={cm.global_clock}, final timestamp={timestamp0}")

      elif msg_type == DATA and fields[1] == -1:
        stopCount += 1
        if stopCount == len(PEERS):
          break
      
        
    # Write log file
    write_log(delivered)
    
    # Send the list of messages to the server (using a TCP socket) for comparison
    print('Sending the list of messages to the server for comparison...')
    clientSock = socket(AF_INET, SOCK_STREAM)
    clientSock.connect((SERVER_ADDR, SERVER_PORT))
    msgPack = pickle.dumps(logList)
    clientSock.send(msgPack)
    clientSock.close()

    exit(0)

# Function to wait for start signal from comparison server:
def waitToStart():
  (conn, addr) = serverSock.accept()
  msgPack = conn.recv(1024)
  msg = pickle.loads(msgPack)
  myself = msg[0]
  nMsgs = msg[1]
  conn.send(pickle.dumps('Peer process '+str(myself)+' started.'))
  conn.close()
  return (myself,nMsgs)

# From here, code is executed when program starts:
registerWithGroupManager()
while 1:
  print('Waiting for signal to start...')
  (myself, nMsgs) = waitToStart()
  print('I am up, and my ID is: ', str(myself))

  if nMsgs == 0:
    print('Terminating.')
    exit(0)

  # Wait for other processes to be ready
  # To Do: fix bug that causes a failure when not all processes are started within this time
  # (fully started processes start sending data messages, which the others try to interpret as control messages) 
  time.sleep(5)

  handshake_done.clear()
  
  # Create receiving message handler
  msgHandler = MsgHandler(recvSocket)
  msgHandler.start()
  print('Handler started')
  time.sleep(1)


  PEERS = getListOfPeers()
  my_ip = gethostname()
  PEERS = [ip for ip in PEERS if ip != my_ip]
  
  # Send handshakes
  # To do: Must continue sending until it gets a reply from each process
  #        Send confirmation of reply
  for addrToSend in PEERS:
    timestamp = send(sendSocket, ('READY', myself), (addrToSend, PEER_UDP_PORT))
    print(f"Handshake sent to {addrToSend}, timestamp={timestamp}")
    #data = recvSocket.recvfrom(128) # Handshadke confirmations have not yet been implemented

  print('Main Thread: Sent all handshakes. handShakeCount=', len(PEERS))
  handshake_done.wait()
  print('Main Thread: All handshakes received — proceeding to message phase.')
  # Send a sequence of data messages to all other processes 
  for msgNumber in range(nMsgs):
    # Wait some random time between successive messages
    time.sleep(random.randrange(10,100)/1000)
    for addrToSend in PEERS:
      timestamp = send(sendSocket, (DATA, myself, msgNumber), (addrToSend, PEER_UDP_PORT))
      print(f"Sent DATA msg {msgNumber} to {addrToSend}, timestamp={timestamp}")

  # Tell all processes that I have no more messages to send
  for addrToSend in PEERS:
    stop_timestamp = send(sendSocket, (DATA, myself, -1), (addrToSend, PEER_UDP_PORT))
    print(f'Stop broadcast sent, timestamp={stop_timestamp}')
  msgHandler.join()
  
