# Distribuidos
Este projeto propõe a construção de um sistema de custódia distribuída para operações na bolsa de valores, no qual o registro de propriedade de ativos financeiros não depende de uma entidade central única, mas sim de uma rede de participantes (como corretoras e bancos).




1. Instalar dependências (uma vez)

Abra um terminal na pasta exchange/:
! cd exchange && pip install -r requirements.txt

2. Gerar as chaves criptográficas (uma vez)

! cd exchange && python scripts/generate_keys.py

3. Iniciar os 4 nós bancários

! cd exchange && python scripts/run_local.py

Isso sobe 4 bancos localmente com SQLite (sem Docker, sem PostgreSQL):

┌────────┬───────────────────────┐
│ Banco  │       Dashboard       │
├────────┼───────────────────────┤
│ bank_0 │ http://localhost:8000 │
├────────┼───────────────────────┤
│ bank_1 │ http://localhost:8001 │
├────────┼───────────────────────┤
│ bank_2 │ http://localhost:8002 │
├────────┼───────────────────────┤
│ bank_3 │ http://localhost:8003 │
└────────┴───────────────────────┘

---
O que você pode fazer na interface

No dashboard de qualquer banco (ex: http://localhost:8000):

- Cards de status — vê chain length, peers conectados, ordens pendentes, nós Bizantinos
- Painel BFT — mostra n=4, f=1, quórum=3 (precisa de 3 votos de aceite)
- Formulário de ordem — seleciona o ativo, lado (compra/venda), quantidade e preço limite
- Tabela de blocos — atualiza a cada 5 segundos
- Tabela de trades — mostra as negociações executadas

---
Via API (curl ou Postman)

# Verificar status do banco
curl http://localhost:8000/api/status

# Submeter ordem de compra
curl -X POST http://localhost:8000/api/orders \
  -H "Content-Type: application/json" \
  -d '{"investor_id":"INV001","stock":"PETR4","side":"buy","quantity":100,"limit_price":35.50}'

# Ver ordens do banco_0
curl http://localhost:8000/api/orders

# Ver trades executados
curl http://localhost:8000/api/trades

# Ver nós Bizantinos detectados
curl http://localhost:8000/api/byzantine

---
Como um bloco é produzido

1. Após 30 segundos ou volume acima da média → o líder inicia produção
2. Sincroniza ordens com todos os peers (flooding)
3. Executa o leilão call-auction
4. Envia o bloco candidato para votação
5. Peers validam e enviam voto assinado com Ed25519
6. Precisa de 3 votos de aceite (quórum BFT 2f+1=3) para confirmar
7. Bloco é commitado em todos os nós e trades são liquidados

Para parar: pressione Ctrl+C no terminal onde o run_local.py está rodando.