"""
Define o roteiro da conversa para o multicast ordenado.

A estrutura é um dicionário onde:
- A chave é o ID do peer (0, 1, 2, ...).
- O valor é uma lista de tuplas.
- Cada tupla contém: (mensagem_a_enviar, deixa).

A 'deixa' é o número de mensagens que o peer deve ter em seu log de mensagens 
entregues antes de enviar a 'mensagem_a_enviar'. Isso garante a ordem causal.
"""

# SCRIPT = {
#     0: [
#         ("Ola Peers, como estao?", 0),
#         ("Tambem estou bem, gostaria de saber se voces estao ocupados hoje.", 3),
#         ("Preciso de ajuda com um trabalho da faculdade.", 5),
#         ("Otimo, nos reunimos mais tarde entao, ate mais galera.", 7)
#     ],
#     1: [
#         ("Estou bem, e voce?", 1),
#         ("Nao estou, por que?", 4),
#         ("Eu ajudo!", 5),
#         ("Ate.", 8)
#     ],
#     2: [
#         ("Estou bem tambem.", 1),
#         ("Foi mal, nao vou poder ajudar, estou ocupado agora.", 6),
#         ("Ate.", 8)
#     ]
# }

SCRIPT = {
    0: [
        ("Ola Peers, como estao?", 0),
        ("Tambem estou bem, gostaria de saber se voces estao ocupados hoje.", 3),
        ("Preciso de ajuda com um trabalho da faculdade.", 5),
        ("Otimo, nos reunimos mais tarde entao, ate mais galera.", 8)
    ],
    1: [
        ("Estou bem, e voce?", 1),
        ("Nao estou, por que?", 4),
        ("Eu ajudo!", 6),
        ("Ate.", 9)
    ],
    2: [
        ("Estou bem tambem.", 2),
        ("Foi mal, nao vou poder ajudar, estou ocupado agora.", 7),
        ("Ate.", 10)
    ]
}

# Calcula o número total de mensagens no roteiro
# Isso será usado pelo servidor de comparação para saber quando a conversa terminou.
TOTAL_MESSAGES_IN_SCRIPT = sum(len(messages) for messages in SCRIPT.values())