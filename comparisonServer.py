from socket import *
import pickle
from constMP import *
import time
import sys

serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', SERVER_PORT))
serverSock.listen(N)

def mainLoop():
	cont = 1
	print("========== SERVIDOR DE COMPARAÇÃO INICIADO ==========")
	print(f"Escutando na porta {SERVER_PORT}...")
	while 1:
		nMsgs = promptUser()
		if nMsgs == 0:
			print("[SERVER] Finalizando servidor...")
			break
		print(f"\n[SERVER] Iniciando rodada {cont} com {nMsgs} mensagens por peer...")
		clientSock = socket(AF_INET, SOCK_STREAM)
		clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
		req = {"op":"list"}
		msg = pickle.dumps(req)
		clientSock.send(msg)
		msg = clientSock.recv(2048)
		clientSock.close()
		peerList = pickle.loads(msg)
		print(f"[SERVER] Lista de peers registrados: {peerList}")
		print(f"[SERVER] Total de peers: {len(peerList)}")
		startPeers(peerList,nMsgs)
		print('Now, wait for the message logs from the communicating peers...')
		waitForLogsAndCompare(nMsgs)
		cont += 1
	serverSock.close()

def promptUser():
	nMsgs = int(input('Enter the number of messages for each peer to send (0 to terminate)=> '))
	return nMsgs

def startPeers(peerList,nMsgs):
	# Connect to each of the peers and send the 'initiate' signal:
	print(f"\n========== INICIANDO PEERS ==========")
	print(f"Iniciando {len(peerList)} peers com {nMsgs} mensagens cada...")
	peerNumber = 0
	for peer in peerList:
		print(f"[SERVER] Iniciando Peer {peerNumber} ({peer})...")
		clientSock = socket(AF_INET, SOCK_STREAM)
		clientSock.connect((peer, PEER_TCP_PORT))
		msg = (peerNumber,nMsgs)
		msgPack = pickle.dumps(msg)
		clientSock.send(msgPack)
		msgPack = clientSock.recv(512)
		response = pickle.loads(msgPack)
		print(f"[SERVER] ✓ {response}")
		clientSock.close()
		peerNumber = peerNumber + 1
	print(f"[SERVER] Todos os peers foram iniciados!")
	print("=" * 40)

def waitForLogsAndCompare(N_MSGS):
	# Loop to wait for the message logs for comparison:
	print(f"\n========== SERVIDOR AGUARDANDO LOGS ==========")
	print(f"Esperando logs de {N} peers...")
	numPeers = 0
	msgs = [] # each msg is a list of tuples (with the original messages received by the peer processes)

	# Receive the logs of messages from the peer processes
	while numPeers < N:
		print(f"[SERVER] Aguardando log do peer {numPeers + 1}/{N}...")
		(conn, addr) = serverSock.accept()
		msgPack = conn.recv(32768)
		received_log = pickle.loads(msgPack)
		print(f"[SERVER] ✓ Log recebido do peer {numPeers + 1} ({addr[0]}): {len(received_log)} mensagens")
		print(f"[SERVER] Conteúdo: {received_log}")
		conn.close()
		msgs.append(received_log)
		numPeers = numPeers + 1

	print(f"\n========== COMPARANDO LOGS ==========")
	print(f"Comparando {len(msgs)} logs com {N_MSGS} mensagens cada...")
	
	# Mostrar todos os logs recebidos
	for i, log in enumerate(msgs):
		print(f"\nPeer {i}: {log}")
	
	unordered = 0

	# Compare the lists of messages
	for j in range(0, min(N_MSGS, len(msgs[0]) if msgs else 0)):
		firstMsg = msgs[0][j] if len(msgs[0]) > j else None
		mismatch = False
		for i in range(1, len(msgs)):
			if len(msgs[i]) > j:
				if firstMsg != msgs[i][j]:
					unordered = unordered + 1
					mismatch = True
					print(f"[ERRO] Posição {j}: Peer0={firstMsg}, Peer{i}={msgs[i][j]}")
					break
			else:
				print(f"[ERRO] Peer {i} tem menos mensagens que esperado")
				mismatch = True
				break
		if mismatch:
			break
	
	print(f"\n========== RESULTADO DA COMPARAÇÃO ==========")
	if unordered == 0:
		print("✓ SUCESSO: Todos os logs são idênticos!")
	else:
		print(f"✗ FALHA: Encontrados {unordered} rounds de mensagens desordenadas")
	print("=" * 50)


# Initiate server:
mainLoop()
