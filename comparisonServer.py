from socket import *
import pickle
from constMP import *
import time
import sys
from script import TOTAL_MESSAGES_IN_SCRIPT

serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', SERVER_PORT))
serverSock.listen(PEER_QTD)

def mainLoop():
	cont = 1
	peerList = []  # Variável para manter a lista de peers
	print("========== SERVIDOR DE COMPARACAO INICIADO ==========")
	print(f"Escutando na porta {SERVER_PORT}...")
	while 1:
		msgs = promptUser()
		if msgs.lower() == 'sair':
			print("[SERVER] Enviando sinal de terminacao para todos os peers...")
			# Enviar sinal de parada para todos os peers
			terminatePeers(peerList)
			print("[SERVER] Finalizando servidor...")
			break
		print(f"\n[SERVER] Iniciando rodada {cont} com o roteiro de {TOTAL_MESSAGES_IN_SCRIPT} definido...")
		clientSock = socket(AF_INET, SOCK_STREAM)
		clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
		req = {"op":"list"}
		msg = pickle.dumps(req)
		clientSock.send(msg)
		msg = clientSock.recv(2048)
		clientSock.close()
		peerList = pickle.loads(msg)

		if len(peerList) != PEER_QTD:
			print(f"[ERRO] O roteiro espera {PEER_QTD} peers, mas {len(peerList)} estao registrados.")
			continue
		print(f"[SERVER] Lista de peers registrados: {peerList}")
		print(f"[SERVER] Total de peers: {len(peerList)}")
		startPeers(peerList)
		print('Agora, aguardando os logs das mensagens dos peers em comunicacao...')
		waitForLogsAndCompare(len(peerList))
		cont += 1
	serverSock.close()

def promptUser():
	msgs = input('Pressione Enter para iniciar o teste com o roteiro (ou digite "sair" para terminar): ')
	return msgs

def startPeers(peerList):
	# Connect to each of the peers and send the 'initiate' signal:
	print(f"\n========== INICIANDO PEERS ==========")
	print(f"Iniciando {len(peerList)} peers para executar o roteiro...")
	peerNumber = 0
	for peer in peerList:
		print(f"[SERVER] Iniciando Peer {peerNumber} ({peer})...")
		try:
			clientSock = socket(AF_INET, SOCK_STREAM)
			clientSock.settimeout(10)  # 10 seconds timeout
			clientSock.connect((peer, PEER_TCP_PORT))
			msg = (peerNumber, -1)
			msgPack = pickle.dumps(msg)
			clientSock.send(msgPack)
			msgPack = clientSock.recv(512)
			response = pickle.loads(msgPack)
			print(f"[SERVER] ✓ {response}")
			clientSock.close()
			peerNumber = peerNumber + 1
		except Exception as e:
			print(f"[SERVER] ✗ Erro ao conectar com peer {peer}: {e}")
			print(f"[SERVER] Peer {peer} pode estar offline ou travado")
	print(f"[SERVER] Tentativa de iniciar todos os peers concluida!")
	print("=" * 40)

def waitForLogsAndCompare(actual_peer_count):
	# Loop to wait for the message logs for comparison:
	print(f"\n========== SERVIDOR AGUARDANDO LOGS ==========")
	print(f"Esperando logs de {actual_peer_count} peers...")
	numPeers = 0
	msgs = [] # each msg is a list of tuples (with the original messages received by the peer processes)

	# Set timeout for socket to avoid waiting forever
	serverSock.settimeout(60)  # 60 seconds timeout
	
	# Receive the logs of messages from the peer processes
	while numPeers < actual_peer_count:
		print(f"[SERVER] Aguardando log do peer {numPeers + 1}/{actual_peer_count}...")
		try:
			(conn, addr) = serverSock.accept()
			msgPack = conn.recv(32768)
			received_log = pickle.loads(msgPack)
			print(f"[SERVER] ✓ Log recebido do peer {numPeers + 1} ({addr[0]}): {len(received_log)} mensagens")
			print(f"[SERVER] Conteudo: {received_log}")
			conn.close()
			msgs.append(received_log)
			numPeers = numPeers + 1
		except socket.timeout:
			print(f"[SERVER] Timeout aguardando logs. Recebidos {numPeers}/{actual_peer_count} logs.")
			break
	
	# Reset timeout
	serverSock.settimeout(None)

	print(f"\n========== COMPARANDO LOGS ==========")
	expected_msgs_per_peer = TOTAL_MESSAGES_IN_SCRIPT
	print(f"Comparando {len(msgs)} logs...")
	print(f"Esperado: {expected_msgs_per_peer} mensagens por peer (total do roteiro)")
	
	# Mostrar todos os logs recebidos
	for i, log in enumerate(msgs):
		print(f"\nPeer {i}: {len(log)} mensagens")
		print(f"Conteudo: {log}")
	
	unordered = 0

	# Verificar se todos os logs têm o mesmo tamanho
	if len(msgs) > 0:
		if not all(len(log) == expected_msgs_per_peer for log in msgs):
			print("[ERRO] Os logs tem tamanhos inconsistentes ou nao batem com o esperado!")
			for i, log in enumerate(msgs):
				if len(log) != expected_msgs_per_peer:
					print(f"  - Peer {i} entregou {len(log)} mensagens, mas esperava {expected_msgs_per_peer}.")
		
		reference_log = msgs[0]
		for i in range(1, len(msgs)):
			if msgs[i] != reference_log:
				unordered += 1
				print(f"[ERRO] Log do Peer {i} e diferente do log de referencia (Peer 0)!")

				for j, (item_ref, item_curr) in enumerate(zip(reference_log, msgs[i])):
					if item_ref != item_curr:
						print(f"  - Primeira diferenca na posicao {j}: Peer 0 -> {item_ref} | Peer {i} -> {item_curr}")
						break
				break
				

	# # Compare the lists of messages
	# max_length = max(len(log) for log in msgs) if msgs else 0
	# for j in range(max_length):
	# 	if len(msgs) == 0:
	# 		break
	# 	firstMsg = msgs[0][j] if len(msgs[0]) > j else None
	# 	mismatch = False
	# 	for i in range(1, len(msgs)):
	# 		if len(msgs[i]) > j:
	# 			if firstMsg != msgs[i][j]:
	# 				unordered = unordered + 1
	# 				mismatch = True
	# 				print(f"[ERRO] Posição {j}: Peer0={firstMsg}, Peer{i}={msgs[i][j]}")
	# 				break
	# 		else:
	# 			print(f"[ERRO] Peer {i} tem menos mensagens que esperado na posição {j}")
	# 			mismatch = True
	# 			break
	# 	if mismatch:
	# 		break
	
	print(f"\n========== RESULTADO DA COMPARACAO ==========")
	if unordered == 0 and len(msgs) == PEER_QTD and all(len(log) == expected_msgs_per_peer for log in msgs):
		print("✓ SUCESSO: Todos os logs sao identicos!")
	else:
		print(f"✗ FALHA: Encontrados {unordered} rounds de mensagens desordenadas")
	print("=" * 50)


def terminatePeers(peerList):
	# Enviar sinal de terminação para todos os peers
	for i, peer in enumerate(peerList):
		try:
			print(f"[SERVER] Enviando sinal de terminacao para Peer {i} ({peer})...")
			clientSock = socket(AF_INET, SOCK_STREAM)
			clientSock.settimeout(5)  # 5 seconds timeout
			clientSock.connect((peer, PEER_TCP_PORT))
			msg = (i, 0)  # 0 mensagens = sinal de terminação
			msgPack = pickle.dumps(msg)
			clientSock.send(msgPack)
			# Não aguarda resposta para terminação
			clientSock.close()
			print(f"[SERVER] ✓ Sinal de terminacao enviado para Peer {i}")
		except Exception as e:
			print(f"[SERVER] Erro ao enviar terminacao para {peer}: {e}")


# Initiate server:
mainLoop()