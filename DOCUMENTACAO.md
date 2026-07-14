# Sistema de Custódia Distribuída para Bolsa de Valores

## Documentação Completa — Funcionamento, Lógica e Implementação

---

## 1. Visão Geral

O sistema simula uma **bolsa de valores distribuída** onde múltiplos bancos/corretoras colaboram para registrar ordens, executar negociações e manter um histórico imutável sem depender de uma entidade central.

Cada banco é um **nó autônomo** que:
- Recebe ordens de compra e venda de investidores
- Propaga essas ordens para os demais nós via flooding
- Participa do protocolo de consenso para confirmar blocos de negociações
- Mantém sua própria cópia da blockchain e banco de dados
- Possui um **gestor humano** que aprova ou rejeita manualmente cada bloco proposto

### Premissa Fundamental

> Nenhum banco sozinho controla o sistema. Uma operação só é confirmada quando a maioria qualificada (quórum BFT) dos nós concorda com ela.

### Duas Interfaces Distintas

O sistema oferece dois painéis web separados, servidos pelo mesmo nó:

| Interface | Rota | Público | Função |
|-----------|------|---------|--------|
| **Gestor do Banco** | `/gestor` | Operadores do banco | Controlar o leilão, **votar em blocos via popup**, monitorar consenso BFT e a rede |
| **Cliente / Investidor** | `/cliente` | Investidores | Enviar ordens de compra/venda, acompanhar carteira e negócios do mercado |

Uma página **portal** (`/`) permite escolher entre as duas interfaces.

### Ciclo de Leilão Controlado pelo Líder

O **líder da rodada** é responsável por controlar o tempo do leilão. A cada intervalo configurável (**padrão: 5 minutos**), o líder fecha a janela de ordens, executa o call-auction e propõe o bloco resultante. Os gestores dos demais bancos então **aprovam ou rejeitam** o bloco antes que ele seja adicionado à blockchain.

---

## 2. Arquitetura do Sistema

```
┌─────────────────────────────────────────────────────────────┐
│                    Rede de Bancos (4 nós)                   │
│                                                             │
│  ┌──────────┐    TCP     ┌──────────┐                       │
│  │  bank_0  │◄──────────►│  bank_1  │                       │
│  │ :9000    │            │ :9001    │                       │
│  │ API:8000 │            │ API:8001 │                       │
│  └────┬─────┘            └────┬─────┘                       │
│       │          TCP          │                             │
│       │◄──────────────────────┼──────────────┐             │
│  ┌────▼─────┐            ┌────▼─────┐        │             │
│  │  bank_2  │◄──────────►│  bank_3  │        │             │
│  │ :9002    │            │ :9003    │        │             │
│  │ API:8002 │            │ API:8003 │        │             │
│  └──────────┘            └──────────┘        │             │
│                                               │             │
│  Cada nó mantém:                              │             │
│  ├── Blockchain (memória)                     │             │
│  ├── Banco SQLite (disco)                     │             │
│  └── Servidor TCP + API HTTP                  │             │
└─────────────────────────────────────────────────────────────┘
```

### Tecnologias Utilizadas

| Camada | Tecnologia |
|--------|-----------|
| Linguagem | Python 3.12 |
| Comunicação P2P | TCP via `asyncio.StreamReader/Writer` |
| API HTTP | FastAPI + Uvicorn |
| Banco de dados local | SQLite via `aiosqlite` (dev) / PostgreSQL via `asyncpg` (prod) |
| Criptografia | Ed25519 via biblioteca `cryptography` |
| Concorrência | `asyncio` (loop de eventos único por nó) |

---

## 3. Estrutura dos Arquivos

```
exchange/
├── bank/
│   ├── __main__.py      → Ponto de entrada: inicia nó + servidor HTTP
│   ├── config.py        → Lê configuração das variáveis de ambiente
│   ├── node.py          → Orquestrador principal do nó bancário
│   ├── blockchain.py    → Estruturas de dados: Block, Order, Trade
│   ├── crypto.py        → Geração/verificação de assinaturas Ed25519
│   ├── gossip.py        → Protocolo de flooding de ordens
│   ├── sync.py          → Sincronização de ordens antes do bloco
│   ├── consensus.py     → Consenso BFT: produção e votação de blocos
│   ├── auction.py       → Algoritmo de call-auction (formação de preço)
│   ├── db.py            → Persistência PostgreSQL
│   ├── db_sqlite.py     → Persistência SQLite (desenvolvimento local)
│   ├── messages.py      → Tipos de mensagens da rede P2P
│   └── api.py           → Endpoints REST + 3 interfaces HTML (portal, gestor, cliente)
├── scripts/
│   ├── generate_keys.py → Gera pares de chaves Ed25519
│   ├── run_local.py     → Inicia múltiplos nós localmente
│   └── run_local_log.py → Igual ao run_local, mas grava logs em logs/bank_N.log
└── requirements.txt
```

---

## 4. Estruturas de Dados (`blockchain.py`)

### Order — Ordem de compra ou venda

```python
Order:
  order_id:    UUID único da ordem
  investor_id: identificador do investidor
  bank_id:     banco que recebeu a ordem
  stock:       código do ativo (ex: "PETR4")
  side:        "buy" ou "sell"
  quantity:    quantidade de ações
  limit_price: preço máximo (compra) ou mínimo (venda)
  timestamp:   ISO8601 UTC
```

### Trade — Negócio executado

```python
Trade:
  trade_id:        UUID único do trade
  stock:           ativo negociado
  buyer_order_id:  ordem de compra que gerou o trade
  seller_order_id: ordem de venda que gerou o trade
  buyer_bank_id:   banco do comprador
  seller_bank_id:  banco do vendedor
  quantity:        quantidade negociada
  price:           preço de clearing
  block_index:     bloco onde foi confirmado
```

### Block — Bloco da blockchain

```python
Block:
  index:           número sequencial do bloco
  timestamp:       momento de criação
  previous_hash:   hash do bloco anterior (encadeamento)
  producer_id:     banco que produziu o bloco
  orders:          lista de ordens incluídas
  trades:          lista de negócios executados
  clearing_prices: preço de fechamento por ativo
  merkle_root:     hash da árvore de Merkle dos trades
  is_eod:          flag de fim de pregão
  block_hash:      SHA-256 do conteúdo do bloco
  signature:       assinatura Ed25519 do produtor
```

### Bloco Gênesis

O primeiro bloco (índice 0) é criado automaticamente na inicialização de cada nó com `previous_hash = "000...000"`. Todos os nós partem do mesmo gênesis, garantindo que a cadeia seja compatível.

---

## 5. Criptografia (`crypto.py`)

Cada banco possui um **par de chaves Ed25519**:
- **Chave privada** (`.priv`): mantida em segredo, usada para assinar
- **Chave pública** (`.pub`): distribuída para todos os nós, usada para verificar

### Geração de chaves

```bash
python scripts/generate_keys.py
# Cria keys/bank_0.priv, keys/bank_0.pub, ..., keys/bank_5.priv, keys/bank_5.pub
```

### Assinatura de blocos

Quando um nó produz um bloco, ele assina o `block_hash`:

```
signature = Ed25519_sign(private_key, block_hash)
```

Qualquer nó pode verificar a autenticidade do bloco usando a chave pública do produtor:

```
valid = Ed25519_verify(public_key[producer_id], block_hash, signature)
```

### Assinatura de votos (Anti-Bizantino)

Votos de consenso também são assinados. Cada nó assina:

```
vote_data = f"{block_index}:{block_hash}:{accepted}"
vote_sig  = Ed25519_sign(private_key, vote_data)
```

Isso impede que um nó mal-intencionado forje votos em nome de outro.

---

## 6. Configuração (`config.py`)

Toda configuração é lida de **variáveis de ambiente**, permitindo rodar múltiplos nós com configs diferentes no mesmo host:

| Variável | Descrição | Exemplo |
|----------|-----------|---------|
| `BANK_ID` | Identificador único do nó | `bank_0` |
| `BANK_HOST` | Endereço de escuta TCP | `127.0.0.1` |
| `BANK_PORT` | Porta TCP P2P | `9000` |
| `API_PORT` | Porta HTTP da API | `8000` |
| `DB_URL` | URL do banco de dados | `sqlite:///./data/bank_0.db` |
| `KEYS_DIR` | Diretório das chaves | `./keys` |
| `PEER_0..N` | Endereços dos peers | `bank_1:127.0.0.1:9001` |
| `AUCTION_INTERVAL_SECONDS` | Intervalo do leilão controlado pelo líder | `300` (5 min) |
| `MANAGER_VOTE_TIMEOUT_SECONDS` | Tempo do gestor para votar antes do fallback automático | `75` |
| `VOTE_TIMEOUT_SECONDS` | Tempo que o líder espera pelos votos dos gestores | `90` |
| `BLOCK_INTERVAL_SECONDS` | Intervalo legado (fallback, geralmente desabilitado) | `9999` |

### Timers do Leilão e da Votação

Como a votação agora depende de uma decisão **humana** (o gestor clica no popup), os timeouts foram ampliados:

- `AUCTION_INTERVAL_SECONDS` (padrão **300s / 5 min**): de quanto em quanto tempo o líder fecha a janela e propõe um bloco.
- `MANAGER_VOTE_TIMEOUT_SECONDS` (padrão **75s**): quanto tempo o gestor tem para clicar Aprovar/Rejeitar. Se não decidir, o voto cai para a recomendação da verificação automática.
- `VOTE_TIMEOUT_SECONDS` (padrão **90s**): quanto tempo o líder espera pelos votos. É maior que o timeout do gestor, garantindo que o fallback automático ocorra antes de o líder desistir.

---

## 7. Protocolo de Flooding (`gossip.py`)

### O que é Flooding

Flooding (inundação) é um protocolo onde cada mensagem recebida é **repassada imediatamente para todos os peers**, garantindo que a informação chegue a toda a rede mesmo que alguns nós estejam offline.

### Por que Flooding em vez de Gossip parcial

O gossip com fanout (enviar para K nós aleatórios) é mais eficiente mas pode deixar nós sem a informação se tiver muita perda. Com flooding:
- **Garantia de propagação**: todos os nós recebem todas as ordens
- **Consistência**: quando o líder iniciar a produção do bloco, todos já têm o mesmo conjunto de ordens
- **Simplicidade**: sem necessidade de controle de quem recebeu o quê

### Implementação

```
Investidor → POST /api/orders → banco recebedor
                                     │
                          ┌──────────▼──────────┐
                          │  GossipManager       │
                          │  broadcast_order()   │
                          │  ↓                   │
                          │  _flood(order)       │
                          │  ↓                   │
                          │  para TODOS peers    │
                          └──────────┬──────────┘
                                     │ TCP
                          ┌──────────▼──────────┐
                          │  Outros bancos       │
                          │  handle_incoming_    │
                          │  gossip()            │
                          │  ↓                   │
                          │  _flood() novamente  │
                          └─────────────────────┘
```

### Deduplicação

Cada nó mantém um `_seen_order_ids: set[str]`. Se uma ordem já foi vista, ela é **descartada sem reencaminhar**, evitando loops infinitos:

```python
if order.order_id in self._seen_order_ids:
    return   # já vimos, não propaga novamente
self._seen_order_ids.add(order.order_id)
# propaga para todos os peers
```

---

## 8. Sincronização de Ordens (`sync.py`)

Antes de produzir um bloco, o líder executa uma **rodada de sincronização** para garantir que todos os nós concordam com o conjunto de ordens a processar.

### Fluxo da Sincronização

```
Líder                           Peers
  │                               │
  │── CLOSE_WINDOW (block_N) ────►│  "fechem a janela de ordens"
  │                               │
  │── SYNC_ORDERS (minhas ordens)►│
  │                               │
  │◄─ SYNC_ORDERS (suas ordens) ──│  cada peer envia suas ordens
  │                               │
  │── SYNC_ACK ──────────────────►│  confirmação de recebimento
  │                               │
  │  [merge + deduplicação]       │
  │  agreed_orders = union(todas) │
  │                               │
```

### Resultado

O `SyncResult.agreed_orders` é a **união deduplicada** de todas as ordens recebidas. Qualquer ordem vista por pelo menos um nó honesto entra no bloco.

Peers que não respondem no timeout (`SYNC_TIMEOUT_SECONDS = 10s`) são listados em `excluded_banks` — o bloco prossegue sem eles.

---

## 9. Algoritmo de Call-Auction (`auction.py`)

O call-auction é o mecanismo de **formação de preço** da bolsa. Diferente de um livro de ordens contínuo, ele processa todas as ordens de uma vez a cada bloco.

### Como funciona

1. **Separa ordens por ativo** (PETR4, VALE3, etc.)
2. Para cada ativo, encontra o **preço de equilíbrio** que maximiza o volume negociado:

```
Para cada preço candidato P:
  demanda(P) = soma das quantidades de compras com limit_price >= P
  oferta(P)  = soma das quantidades de vendas com limit_price <= P
  volume(P)  = min(demanda, oferta)

Escolhe P* = argmax(volume)
```

3. Executa os trades com **alocação pro-rata** quando múltiplos vendedores/compradores competem no mesmo nível de preço

4. Retorna `trades[]` e `clearing_prices{}` (preço de fechamento por ativo)

### Prevenção de Auto-Negociação (Self-Trade)

Um investidor **não pode negociar consigo mesmo**. Se o mesmo `investor_id` tiver uma ordem de compra e uma de venda que se cruzariam, elas **não geram trade** — só há negócio entre investidores **diferentes**.

Na alocação, ao processar cada compra, os vendedores com o **mesmo `investor_id`** do comprador são excluídos do casamento:

```python
tier = [
    s for s in sells_by_price[sell_price]
    if sell_rem[s.order_id] > 0 and s.investor_id != buy.investor_id
]
```

Se após essa exclusão não sobrar nenhum trade executável para o ativo, nenhum preço de clearing é registrado (evita preço "fantasma"). Como a regra é **determinística** e todos os nós têm as mesmas ordens (com `investor_id`), a verificação de consenso reproduz exatamente o mesmo resultado.

> Exemplo: GUSTAVO envia compra de 100 PETR4 @ R$36 e venda de 100 PETR4 @ R$34. Sem outro investidor, o leilão gera **0 trades**. Se MARIA também vende 100 @ R$34, GUSTAVO compra de MARIA (não de si mesmo).

### Exemplo

```
Ordens para PETR4:
  Compra: 100 ações @ R$36,00  (aceita qualquer preço ≤ 36)
  Compra: 200 ações @ R$35,00
  Venda:  150 ações @ R$34,50  (aceita qualquer preço ≥ 34,50)
  Venda:  100 ações @ R$35,50

Preço R$35,00:
  demanda = 300 (ambas as compras), oferta = 150 → volume = 150

Preço R$35,50:
  demanda = 100 (só a compra @36), oferta = 250 → volume = 100

Clearing em R$35,00 → volume = 150 ações negociadas
```

---

## 10. Consenso com Tolerância Bizantina (`consensus.py`)

### O que é Tolerância Bizantina

Um **nó Bizantino** é um nó que se comporta de forma maliciosa ou arbitrária: pode mandar mensagens falsas, votar diferente para nós diferentes (equivocação), ou enviar assinaturas inválidas.

O sistema garante que mesmo com nós Bizantinos, os nós honestos chegam ao mesmo consenso.

### Parâmetros BFT

Para `n` bancos:
- **f = floor((n-1)/3)** → número máximo de falhas Bizantinas toleradas
- **Quórum = 2f+1** → votos mínimos para confirmar um bloco

| n (bancos) | f (falhas) | Quórum |
|------------|-----------|--------|
| 3 | 0 | 1 |
| 4 | 1 | **3** ← configuração padrão |
| 6 | 1 | 3 |
| 7 | 2 | 5 |

Com 4 bancos (padrão do `run_local.py`): tolera **1 banco Bizantino**, precisa de **3 votos** para confirmar.

### Eleição do Líder

A cada bloco, o líder é determinado de forma **determinística e rotativa**:

```python
leader = sorted_bank_ids[block_index % n]

# Bloco 0 → bank_0 é líder
# Bloco 1 → bank_1 é líder
# Bloco 2 → bank_2 é líder
# ...
```

Não há eleição — todos sabem quem é o líder para cada bloco sem comunicação extra. O líder também é quem **controla o timer do leilão** (ver Seção 11).

### Fluxo Completo de Produção de Bloco (com votação humana)

```
Líder                         Peers (gestores humanos)
  │                             │
  │  [timer do leilão zera]     │
  │  [verifica se é líder]      │
  │                             │
  ├─── CLOSE_WINDOW ───────────►│
  │                             │
  ├─── [sync round] ───────────►│  (ver Seção 8)
  │◄── [ordens coletadas] ──────┤
  │                             │
  │  [executa call-auction]     │
  │  [calcula merkle_root]      │
  │  [assina bloco com priv_key]│
  │                             │
  ├─── BLOCK_CANDIDATE ────────►│  "valide este bloco"
  │                             │
  │                    [verificação AUTOMÁTICA gera recomendação:]
  │                    - previous_hash correto?
  │                    - block_hash válido?
  │                    - merkle_root correto?
  │                    - assinatura do produtor válida?
  │                    - auction reproduz o mesmo resultado?
  │                             │
  │                    ┌─────────▼──────────┐
  │                    │  🗳️ POPUP no painel │
  │                    │  do gestor          │
  │                    │  [Aprovar/Rejeitar] │  ← decisão HUMANA
  │                    └─────────┬──────────┘
  │                             │
  │◄─── BLOCK_VOTE (assinado) ──┤  voto do gestor, assinado Ed25519
  │                             │
  │  [valida assinatura do voto]│
  │  [detecta equivocação]      │
  │  [conta: aceites ≥ quórum(3)?]
  │                             │
  ├─── BLOCK_COMMIT ───────────►│  se aprovado por quórum
  │                             │
  [todos adicionam à blockchain]
  [todos liquidam os trades no DB]

  │  Se REJEITADO (sem quórum):  │
  ├─── BLOCK_REJECTED ─────────►│  rotaciona líder (round++)
  │                             │  próximo banco faz novo leilão
```

### Votação Humana (Human-in-the-Loop)

Diferente de uma blockchain totalmente automática, aqui **cada gestor de banco aprova ou rejeita manualmente** o bloco proposto pelo líder. O fluxo:

1. Quando o `BLOCK_CANDIDATE` chega, o nó **não vota sozinho**. Ele executa a verificação automática (hash, assinatura, reprodução do leilão) apenas para gerar uma **recomendação**.
2. O bloco fica pendente em `_pending_manager_vote`, exposto via `GET /api/pending-vote`.
3. O painel do gestor detecta o bloco pendente e **abre um popup** mostrando os detalhes (bloco, trades, hash) e a recomendação automática.
4. O gestor clica **✅ Aprovar** ou **❌ Rejeitar** → `POST /api/cast-vote`.
5. O voto é então **assinado com Ed25519** e enviado ao líder.
6. **Fallback**: se o gestor não decidir em `MANAGER_VOTE_TIMEOUT_SECONDS` (75s), o sistema usa a recomendação automática, garantindo que a rede não trave sem operadores humanos.

```python
async def _await_manager_decision(candidate, sender, auto_ok):
    fut = loop.create_future()
    self._manager_vote_future = fut
    self._pending_manager_vote = { ...dados do bloco..., "auto_recommendation": auto_ok }
    try:
        decision = await asyncio.wait_for(fut, timeout=manager_vote_timeout_seconds)
    except asyncio.TimeoutError:
        decision = auto_ok   # fallback para a verificação automática
    return decision
```

> **O líder não recebe popup** — ele é o *propositor* do bloco e conta como aceite implícito. Quem vota são os demais gestores, como manda o modelo BFT (proponente + votantes).

### Acompanhamento da Votação em Tempo Real

O `ConsensusManager` mantém o estado de cada voto (`pending` → `accepted` / `rejected` / `timeout` / `byzantine`), exposto via `GET /api/vote-status`. O painel do gestor renderiza cartões por banco que mudam de cor conforme os votos chegam, permitindo ver o consenso se formar ao vivo.

### Rejeição de Bloco → Rotação de Líder (Round/View Change)

Quando um bloco **não atinge o quórum** (gestores rejeitam), o sistema **não re-propõe o mesmo bloco** com o mesmo líder. Em vez disso, a liderança **rotaciona para o próximo banco**, que realiza um **novo leilão**.

Isso é implementado com um contador de **round** (semelhante ao *view change* do PBFT) que desloca o líder para a mesma altura de bloco:

```python
leader = sorted_bank_ids[(block_index + round) % n]
```

Fluxo na rejeição:

```
1. Líder conta os votos → quórum NÃO atingido
2. Líder: consensus.advance_round()          # round += 1 → novo líder
3. Líder → BLOCK_REJECTED{block_index, round} # avisa todos os nós
4. Todos os nós: consensus.set_round(round)   # sincronizam o mesmo líder
5. Janela de leilão reinicia → novo líder conduz outro leilão
   (as ordens pendentes NÃO são descartadas — voltam ao próximo leilão)
```

Quando um bloco é **finalmente aprovado e commitado**, `commit_block` chama `reset_round()` — a altura avança e a rotação normal (`block_index % n`) recomeça do zero.

| Evento | round | Líder (n=4) |
|--------|-------|-------------|
| Bloco 1, tentativa inicial | 0 | bank_1 |
| Rejeitado → rotaciona | 1 | bank_2 |
| Rejeitado → rotaciona | 2 | bank_3 |
| Aprovado e commitado | reset → 0 | (próxima altura) |

Assim, um bloco só entra na blockchain **com a aprovação explícita** dos gestores; enquanto rejeitado, cada banco tem sua vez de propor, sem repetir indefinidamente o mesmo proponente.

### Detecção de Nós Bizantinos

O sistema detecta duas formas de comportamento Bizantino:

**1. Assinatura inválida no voto:**
```python
vote_data = f"{block_index}:{block_hash}:{accepted}"
if not verify_block(peer_key, vote_data, payload.signature):
    self._byzantine_nodes.add(sender_id)
    return  # voto ignorado
```

**2. Equivocação (dois votos conflitantes):**
```python
if fut.done():
    prev = fut.result()
    if prev.accepted != payload.accepted:
        # mesmo nó votou sim E não para o mesmo bloco
        self._byzantine_nodes.add(sender_id)
```

Nós detectados como Bizantinos são **excluídos do quórum** em todos os blocos futuros e listados em `/api/byzantine`.

---

## 11. Nó Bancário (`node.py`)

O `BankNode` é o **orquestrador central** — conecta todos os componentes e gerencia o ciclo de vida.

### Inicialização

```python
async def start():
    1. Carrega chave privada e chaves públicas dos peers
    2. Inicializa banco de dados (SQLite ou PostgreSQL)
    3. Abre servidor TCP na porta configurada
    4. Inicia tarefas em background:
       - peer_connect_loop()   → tenta conectar a peers a cada 5s
       - heartbeat_loop()      → envia heartbeat a cada 5s
       - auction_timer_loop()  → líder produz bloco quando o timer zera
```

### Gatilho de Produção de Bloco — Timer Controlado pelo Líder

O gatilho de bloco é **exclusivo do líder da rodada**. A cada 5 segundos, o `auction_timer_loop` verifica:

```python
block_index = len(self.blockchain)
leader_id   = consensus.get_current_leader(block_index)

if leader_id != self.bank_id:
    continue   # não sou o líder → não faço nada

elapsed = agora - self._auction_window_opened
if elapsed >= AUCTION_INTERVAL_SECONDS:   # padrão 300s / 5 min
    produzir_bloco()                       # fecha janela, roda leilão, propõe bloco
```

Pontos-chave:
- **Somente o líder** fecha a janela de ordens e propõe o bloco. Os demais nós apenas recebem o `BLOCK_COMMIT`.
- A janela reinicia (`_auction_window_opened`) a cada bloco confirmado, iniciando a contagem para o próximo.
- O endpoint `GET /api/auction-status` expõe o tempo restante, permitindo o **countdown** nos painéis.

### Disparo Manual do Leilão

Além do timer automático, o endpoint `POST /api/trigger-block` permite **forçar** um leilão imediatamente (útil para demonstração). Ele chama `_trigger_block_production(force=True)`, que ignora a verificação de liderança e produz o bloco na hora.

### Detecção de Peers Mortos

O heartbeat loop remove peers que não respondem há mais de `PEER_TIMEOUT_SECONDS = 20s`:

```python
stale = [pid for pid, t in last_seen.items() if now - t > 20]
for pid in stale:
    peer_writers.pop(pid)  # desconecta peer
```

---

## 12. Camada de Persistência (`db.py` / `db_sqlite.py`)

### Esquema do Banco de Dados

```sql
blocks       → histórico de blocos (raw_json completo)
orders       → ordens submetidas (status: pending/matched/partial/expired/cancelled; filled_quantity registra quanto foi executado)
trades       → negócios executados
investors    → cadastro de investidores (saldo em cash)
portfolios   → carteira por investidor/ativo
price_history→ preço de fechamento por bloco
daily_ohlc   → OHLC diário (abertura/máxima/mínima/fechamento/volume)
```

### Factory Pattern

O código usa uma função factory para selecionar automaticamente o backend:

```python
def make_database(db_url: str):
    if db_url.startswith("sqlite"):
        return SqliteDatabase(db_url)   # desenvolvimento local
    return Database(db_url)              # PostgreSQL (produção/Docker)
```

A troca é transparente — ambas implementam exatamente a mesma interface.

### Liquidação pós-bloco (`persist_block`)

Quando um bloco é commitado, o banco persiste em ordem:
1. Insere o bloco (raw JSON)
2. Insere as ordens com status `pending`
3. Garante que cada investidor da ordem existe (`ensure_investor`, saldo inicial R$ 100.000)
4. Calcula `filled_quantity` por ordem somando os trades em que ela aparece (compra ou venda)
5. Define o status final da ordem:
   - `filled_quantity >= quantity` → `matched` (execução total)
   - `0 < filled_quantity < quantity` → `partial` (o restante da quantidade é cancelado — o leilão é single-shot, não há retentativa em rodadas futuras)
   - `filled_quantity == 0` → `expired` (ou `cancelled` se for EOD)
6. Insere os trades
7. Liquida cada trade: debita o comprador e credita o vendedor em `cash_balance`, atualiza `portfolios` (ações compradas/vendidas)
8. Atualiza `price_history` com preço e volume
9. Se for EOD: insere OHLC do dia

---

## 13. Interface HTTP (`api.py`)

### Páginas HTML

| Rota | Descrição |
|------|-----------|
| GET `/` | **Portal** — escolha entre as interfaces Gestor e Cliente |
| GET `/gestor` | **Painel do Gestor** — leilão, votação de blocos, consenso BFT |
| GET `/cliente` | **Painel do Cliente** — ordens, carteira, mercado |
| GET `/docs` | Documentação interativa Swagger |

### Endpoints REST

| Método | Rota | Descrição |
|--------|------|-----------|
| GET | `/api/status` | Status do nó: chain, peers, BFT, gossip |
| GET | `/api/peers` | Lista de peers conectados |
| GET | `/api/blocks?limit=20` | Blocos recentes da chain |
| GET | `/api/blocks/{index}` | Bloco específico com todos os dados |
| POST | `/api/orders` | Submeter nova ordem |
| GET | `/api/orders?limit=50` | Ordens registradas no banco |
| GET | `/api/trades?limit=50` | Trades executados |
| GET | `/api/portfolio/{investor_id}` | Carteira de um investidor |
| GET | `/api/stocks` | Lista de ativos disponíveis |
| GET | `/api/byzantine` | Nós Bizantinos detectados |
| GET | `/api/pending-orders` | Ordens pendentes + líder atual |
| GET | `/api/auction-status` | Timer do leilão: tempo restante, líder, intervalo |
| GET | `/api/vote-status` | Estado ao vivo da votação (por banco) |
| **GET** | **`/api/pending-vote`** | **Bloco aguardando a decisão do gestor deste banco** |
| **POST** | **`/api/cast-vote`** | **Gestor registra voto (aprovar/rejeitar)** |
| POST | `/api/trigger-block` | Força um leilão imediatamente |
| POST | `/api/seed-test` | Cria pares de ordens compra+venda para teste |

### Submissão de Ordem (POST /api/orders)

```json
{
  "investor_id": "INV001",
  "stock": "PETR4",
  "side": "buy",
  "quantity": 100,
  "limit_price": 35.50
}
```

Resposta:
```json
{
  "order_id": "uuid-gerado",
  "status": "submitted",
  "bank_id": "bank_0"
}
```

### Registro de Voto (POST /api/cast-vote)

Chamado pelo popup do painel do gestor:

```json
{ "approve": true }
```

Resposta:
```json
{ "recorded": true, "approved": true, "bank_id": "bank_1" }
```

Se não houver bloco aguardando votação, retorna **HTTP 409**.

### Painel do Gestor (`/gestor`)

Interface completa para operadores do banco, com **polling automático a cada 2 segundos**:

```
┌───────────────────────────────────────────────────────┐
│ Painel do Gestor — bank_0  [Flooding][Online]  ↗Cliente│
├──────────┬──────────┬──────────┬──────────────────────┤
│ Chain    │ Peers    │ Pendentes│ Bizantinos           │
│ 42 blocos│ 3/3      │ 5 ordens │ 0                    │
├──────────────────────────┬────────────────────────────┤
│ ⏱️ TIMER DO LEILÃO        │ 🗳️ VOTAÇÃO DOS GESTORES    │
│      04:37                │ [bank_0 ✅][bank_1 ⏳]      │
│ ████████░░░ Líder: bank_2 │ [bank_2 ✅][bank_3 ❌]      │
│ [⚡ Forçar][🌱 Teste]      │ Resultado: APROVADO 3/3    │
├──────────────────────────┴────────────────────────────┤
│ BFT: n=4 │ f=1 │ quórum=3     Peers: [b1●][b2●][b3●]   │
├───────────────────────────────────────────────────────┤
│ Blocos Recentes / Trades Executados                   │
└───────────────────────────────────────────────────────┘

     Quando o líder propõe um bloco, surge o POPUP:
     ┌──────────────────────────────────────┐
     │ 🗳️ Novo Bloco para Votação           │
     │            75  (countdown)           │
     │ Bloco #42 · Proposto por: bank_2     │
     │ Trades: 8 · Hash: a1b2c3...          │
     │ 🔍 Verificação automática: VÁLIDO    │
     │   [❌ Rejeitar]      [✅ Aprovar]     │
     └──────────────────────────────────────┘
```

Componentes do painel do gestor:
- **Timer do leilão** com countdown e barra de progresso (`/api/auction-status`)
- **Botões** de forçar leilão e criar ordens de teste
- **Painel de votação** ao vivo, com cartões por banco (`/api/vote-status`)
- **Popup de votação** que aparece quando há bloco pendente (`/api/pending-vote` → `/api/cast-vote`)
- Cards de status, BFT, peers, blocos e trades

### Painel do Cliente (`/cliente`)

Interface simplificada para investidores — **sem detalhes de consenso**:

```
┌───────────────────────────────────────────────────────┐
│ 💹 Home Broker   Banco: bank_0   ID:[Gustavo][Entrar]  │
├──────────────┬──────────────┬─────────────────────────┤
│ Saldo        │ Minhas Ordens│ Próximo Leilão          │
│ R$ 100.000   │ 3 pendentes  │    02:14                │
├──────────────┴──────────────┴─────────────────────────┤
│ Enviar Ordem                                          │
│ Ativo:[PETR4▼] Qtd:[100] Preço:[35.00] [▲Comprar][▼Vender]
├───────────────────────────┬───────────────────────────┤
│ Minhas Ordens             │ Minha Carteira            │
│ PETR4 COMPRA 100 pendente │ PETR4  200                │
├───────────────────────────┴───────────────────────────┤
│ Mercado — Últimos Negócios                            │
└───────────────────────────────────────────────────────┘
```

Componentes do painel do cliente:
- Campo de **ID do investidor** (salvo em `localStorage`)
- **Saldo e carteira** (`/api/portfolio/{investor_id}`)
- Botões **Comprar / Vender** (`/api/orders`)
- **Minhas ordens** filtradas por investidor, com status (pendente/executada/expirada)
- **Countdown** do próximo leilão e **mercado** com últimos negócios

Ambos os painéis são **Single Page Applications** em HTML/CSS/JavaScript puro (sem frameworks externos), servidas diretamente pela FastAPI.

---

## 14. Comunicação P2P (`messages.py`)

Todos os nós se comunicam via **TCP com mensagens JSON delimitadas por `\n`**.

### Tipos de Mensagem

| Tipo | Direção | Descrição |
|------|---------|-----------|
| `ORDER_GOSSIP` | todos → todos | Propaga uma ordem por flooding |
| `CLOSE_WINDOW` | líder → peers | Sinaliza início do ciclo de bloco |
| `SYNC_ORDERS` | todos → líder | Envia ordens pendentes para sincronização |
| `SYNC_ACK` | líder → peers | Confirma recebimento das ordens |
| `BLOCK_CANDIDATE` | líder → peers | Bloco candidato aguardando votos |
| `BLOCK_VOTE` | peers → líder | Voto do gestor (aceite/rejeição) assinado |
| `BLOCK_COMMIT` | líder → peers | Bloco aprovado, commitar na chain |
| `BLOCK_REJECTED` | líder → peers | Bloco rejeitado — rotaciona líder (novo round) |
| `CHAIN_SYNC_REQUEST` | novo nó → peer | Solicita blocos que está faltando |
| `CHAIN_SYNC_RESPONSE` | peer → novo nó | Envia os blocos solicitados |
| `HEARTBEAT` | todos → todos | Sinal de vida a cada 5 segundos |

### Formato de uma Mensagem

```json
{
  "msg_type": "ORDER_GOSSIP",
  "sender_id": "bank_0",
  "msg_id": "uuid-da-mensagem",
  "timestamp": "2026-06-30T15:30:00Z",
  "payload": {
    "order": {
      "order_id": "...",
      "stock": "PETR4",
      ...
    }
  }
}
```

---

## 15. Fluxo Completo de uma Ordem

```
1. SUBMISSÃO
   Investidor → POST /api/orders (banco_0)
   └── banco_0.gossip.broadcast_order(ordem)

2. FLOODING
   bank_0 → FLOOD → bank_1, bank_2, bank_3
   bank_1 → FLOOD → bank_0, bank_2, bank_3  (bank_0 já viu, descarta)
   bank_2 → FLOOD → bank_0, bank_1, bank_3  (já viram, descartam)
   [todos têm a ordem em _pending]

3. GATILHO DE BLOCO (timer do leilão zera, padrão 5 min)
   bank_N é líder do bloco atual e controla o timer
   └── envia CLOSE_WINDOW para todos

4. SINCRONIZAÇÃO
   líder → SYNC_ORDERS (suas ordens)
   peers → SYNC_ORDERS (suas ordens)
   líder monta agreed_orders = union(todas as ordens)

5. CALL-AUCTION
   agreed_orders → run_call_auction()
   └── encontra preço de clearing
   └── executa trades
   └── cria Block com hash + assinatura

6. VOTAÇÃO BFT (com decisão humana)
   líder → BLOCK_CANDIDATE (bloco)
   cada peer:
     - verifica automaticamente (hash, merkle, assinatura, auction) → recomendação
     - mostra POPUP ao gestor com os detalhes
     - gestor clica ✅ Aprovar ou ❌ Rejeitar (fallback automático em 75s)
   peers → BLOCK_VOTE (decisão do gestor, assinada com priv_key)
   líder valida assinaturas dos votos
   aceites ≥ quórum(3) → APROVADO
   senão → REJEITADO: líder envia BLOCK_REJECTED, round++,
           liderança rotaciona p/ o próximo banco, que faz NOVO leilão
           (ordens continuam pendentes; nada é commitado)

7. COMMIT (somente se aprovado)
   líder → BLOCK_COMMIT
   todos: blockchain.append(block)
         db.persist_block(block)
         gossip.clear_pending_orders()
   ordem fica com status "matched" no banco

8. RESULTADO
   Trade visível em /api/trades
   Bloco visível em /api/blocks
```

---

## 16. Recovery e Sincronização de Chain

Quando um nó se reconecta após ficar offline, ele pode estar com a chain desatualizada. O protocolo de sincronização:

```
Nó recuperando           Peer ativo
      │                      │
      │── CHAIN_SYNC_REQUEST ►│  "me dê blocos a partir do índice 5"
      │                      │
      │◄─ CHAIN_SYNC_RESPONSE ┤  envia blocos [5, 6, 7, ..., N]
      │                      │
      │  [valida e aplica     │
      │   cada bloco]         │
```

O nó valida cada bloco recebido (hash, previous_hash, merkle_root) antes de adicionar à chain local.

---

## 17. Execução Local

### Pré-requisitos

```bash
pip install -r requirements.txt
```

### Inicialização (uma única vez)

```bash
cd exchange
python scripts/generate_keys.py
```

### Rodar o sistema

```bash
python scripts/run_local.py           # 4 bancos (padrão, BFT f=1)
python scripts/run_local.py --banks 6 # 6 bancos (mesmo f=1, mais redundância)
python scripts/run_local.py --clean   # apaga dados e recomeça do zero

# Variante que grava logs em logs/bank_N.log (útil para depuração):
python scripts/run_local_log.py --banks 4 --clean
```

### Acessar as interfaces

Cada banco serve o **portal** e as duas interfaces em sua porta HTTP:

```
http://localhost:8000/          → Portal (bank_0)
http://localhost:8000/gestor    → Painel do Gestor (bank_0)
http://localhost:8000/cliente   → Painel do Cliente (bank_0)

http://localhost:8001/gestor    → Painel do Gestor (bank_1)
http://localhost:8002/gestor    → Painel do Gestor (bank_2)
http://localhost:8003/gestor    → Painel do Gestor (bank_3)
```

### Roteiro de Demonstração da Votação

1. Abra o `/cliente` e envie ordens (ou use **🌱 Ordens de Teste** no `/gestor`).
2. Abra o `/gestor` de **cada banco** em abas separadas (portas 8000–8003).
3. No painel do líder, clique **⚡ Forçar Leilão Agora** (ou espere o timer de 5 min).
4. Nas abas dos **demais** bancos, o **popup 🗳️ aparece** → clique Aprovar/Rejeitar.
5. Com quórum (2f+1 = 3 aprovações), o bloco entra na blockchain; caso contrário, é rejeitado.

### Parar

`Ctrl+C` no terminal — todos os processos são encerrados.

---

## 18. Implantação com Docker (Produção)

Para ambientes com Docker instalado, o sistema roda com PostgreSQL:

```bash
cd exchange
python scripts/generate_keys.py
docker compose up --build
```

O `docker-compose.yml` sobe:
- 6 instâncias PostgreSQL (uma por banco)
- 6 nós bancários (bank_0 a bank_5)
- Rede interna Docker `exchange_net`

Portas expostas: TCP 9000-9005 (P2P) e HTTP 8000-8005 (API/dashboard).

---

## 19. Propriedades de Segurança

| Propriedade | Mecanismo |
|-------------|-----------|
| **Autenticidade de blocos** | Assinatura Ed25519 do produtor |
| **Integridade da chain** | Hash encadeado (cada bloco inclui hash do anterior) |
| **Integridade dos trades** | Árvore de Merkle dos trades no bloco |
| **Autenticidade dos votos** | Assinatura Ed25519 do votante |
| **Resistência a equivocação** | Detecção de votos conflitantes do mesmo nó |
| **Resistência a replay** | `msg_id` UUID único por mensagem + `_seen_order_ids` |
| **Propagação completa** | Flooding garante que todos recebem todas as ordens |
| **Consenso seguro** | Quórum BFT 2f+1 impede aprovação com nós desonestos |
| **Autorização humana** | Gestor de cada banco aprova/rejeita blocos manualmente antes do commit |
| **Segregação de acesso** | Interfaces separadas: gestor (consenso) e cliente (ordens) |

---

## 20. Ativos Disponíveis

O sistema opera com 15 ativos da bolsa brasileira:

`PETR4` `VALE3` `ITUB4` `BBDC4` `ABEV3` `WEGE3` `RENT3` `BBAS3` `SUZB3` `RDOR3` `RADL3` `EGIE3` `LREN3` `HAPV3` `MGLU3`
