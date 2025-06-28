[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/TyBiAFsA)
# Trabalho Final - Sistemas Distribuídos

A princípio está sendo implementado uma solução de multicast totalmente ordenado utilizando o conceito de relógios lógicos de Lamport com algoritmo de ordenação por fila de prioridade.

Códigos foram pensando para rodar em instâncias AWS EC2 de regiões diferentes.

## Setup e Execução
> Mude `constMP.py` para ajustar os endereços IP do servidor e do GroupManager.

> Abra um terminal para `comparisonServer.py` e outro para `GroupMngr.py`.
> Caso a maquina do servidor já esteja usando a porta, use o comando `sudo pkill -f python3`

> Abra PEER_QTD terminais para o número de peers definido em `constMP.py` e execute `peerCommunicatorUDP` em cada um.

Por fim, digite quantas mensagens devem ser enviadas no processo no terminal do `comparisonServer.py`.