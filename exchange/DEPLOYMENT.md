# Deployment em Duas Máquinas

Este guia explica como rodar o cluster de 6 bancos dividido entre duas máquinas físicas na mesma LAN, e como validar que o sistema é de fato distribuído.

## Arquitetura

- **Máquina 1** roda `bank_0`, `bank_1`, `bank_2` (portas TCP 9000–9002, APIs 8000–8002)
- **Máquina 2** roda `bank_3`, `bank_4`, `bank_5` (portas TCP 9003–9005, APIs 8003–8005)
- Cada banco tem seu próprio PostgreSQL (interno à máquina, não exposto)
- Os peers remotos são alcançados pelo IP da outra máquina via variáveis `MACHINE1_IP` / `MACHINE2_IP`

## Pré-requisitos

- Docker + Docker Compose instalados nas duas máquinas
- As duas máquinas na mesma LAN (conseguem se pingar)
- Firewall liberando TCP de entrada: 9000–9002 na máquina 1, 9003–9005 na máquina 2

## Passo 1 — Gerar as chaves (uma vez, na máquina 1)

```bash
cd exchange
python scripts/generate_keys.py
```

Copie o diretório `keys/` inteiro para o mesmo caminho na máquina 2 (pendrive, scp, etc.).
Todas as réplicas precisam das MESMAS chaves — se divergirem, a verificação de assinatura dos votos falha.

## Passo 2 — Descobrir o IP de cada máquina

**Windows (PowerShell):**
```powershell
Get-NetIPAddress -AddressFamily IPv4 | Where-Object { $_.InterfaceAlias -like "*Wi-Fi*" -or $_.InterfaceAlias -like "*Ethernet*" } | Select-Object IPAddress, InterfaceAlias
```

**Linux/macOS:**
```bash
ip addr | grep "inet "     # ou: ifconfig
```

Use o IP da interface real da LAN (Wi-Fi/Ethernet) — **não** use os IPs virtuais de WSL, VirtualBox ou VPN.

## Passo 3 — Liberar o firewall (Windows)

Na máquina 1 (PowerShell como Administrador):
```powershell
New-NetFirewallRule -DisplayName "Exchange Banks M1" -Direction Inbound -Protocol TCP -LocalPort 9000-9002 -Action Allow
```

Na máquina 2:
```powershell
New-NetFirewallRule -DisplayName "Exchange Banks M2" -Direction Inbound -Protocol TCP -LocalPort 9003-9005 -Action Allow
```

## Passo 4 — Subir a máquina 1 (bancos 0–2)

**Windows (PowerShell):**
```powershell
cd exchange
$env:MACHINE2_IP = "192.168.1.20"   # troque pelo IP real da máquina 2
docker compose -f docker-compose.machine1.yml up --build -d
```

**Linux/macOS:**
```bash
export MACHINE2_IP=192.168.1.20
docker compose -f docker-compose.machine1.yml up --build -d
```

## Passo 5 — Subir a máquina 2 (bancos 3–5)

**Windows (PowerShell):**
```powershell
cd exchange
$env:MACHINE1_IP = "192.168.1.10"   # troque pelo IP real da máquina 1
docker compose -f docker-compose.machine2.yml up --build -d
```

**Linux/macOS:**
```bash
export MACHINE1_IP=192.168.1.10
docker compose -f docker-compose.machine2.yml up --build -d
```

## Passo 6 — Validar

Em qualquer uma das máquinas, verifique que cada banco enxerga os 5 peers
(troque `localhost` pelo IP da outra máquina para consultar os bancos remotos):

```bash
curl http://localhost:8000/api/status
# esperado: "connected_peers":["bank_1","bank_2","bank_3","bank_4","bank_5"]
```

Dashboards no navegador: `http://<IP da máquina 1>:8000` a `8002` e `http://<IP da máquina 2>:8003` a `8005`.

### Teste ponta-a-ponta entre máquinas

1. Envie uma **compra** num banco da máquina 1 e uma **venda casável** num banco da máquina 2:
   ```bash
   curl -X POST http://<IP_M1>:8000/api/orders -H "Content-Type: application/json" \
     -d '{"investor_id":"ALICE","stock":"PETR4","side":"buy","quantity":100,"limit_price":36.00}'
   curl -X POST http://<IP_M2>:8005/api/orders -H "Content-Type: application/json" \
     -d '{"investor_id":"BOB","stock":"PETR4","side":"sell","quantity":100,"limit_price":34.00}'
   ```
2. Confira que as 2 ordens aparecem como pendentes nos 6 bancos (gossip cruzou as máquinas).
3. No dashboard do líder atual (`/api/auction-status` mostra quem é), clique em
   **"⚡ Forçar Leilão Agora"** — ou `POST /api/trigger-block`.
4. O popup de votação aparece nos dashboards dos gestores DAS DUAS máquinas; aprove.
5. Verifique que `chain_length` incrementou nos 6 bancos e que o trade registra
   comprador e vendedor de bancos em máquinas diferentes.

### Teste de tolerância a falhas

1. Derrube um banco: `docker stop <container do bank_4>` na máquina 2.
2. Produza um bloco novo — o consenso continua (quórum 3 de 6).
3. Religue o banco: `docker start ...` — ele restaura a cadeia do Postgres local
   e busca os blocos perdidos dos peers via chain sync (mesmo os da outra máquina).

## Simulação das duas máquinas num único computador

Para testar a topologia multi-máquina sem um segundo computador, rode os dois
compose files como **projetos Docker isolados** (redes bridge separadas). O tráfego
entre os clusters é forçado a sair pelas portas publicadas no IP da LAN do host —
o mesmo caminho de rede de duas máquinas físicas:

```powershell
cd exchange
$IP = "<IP da LAN deste computador>"   # ex.: 192.168.87.28

$env:MACHINE2_IP = $IP
docker compose -p machine1 -f docker-compose.machine1.yml up --build -d

$env:MACHINE1_IP = $IP
docker compose -p machine2 -f docker-compose.machine2.yml up --build -d
```

Para derrubar tudo: `docker compose -p machine1 -f docker-compose.machine1.yml down`
(e o equivalente com `machine2`).

## Resultado da validação (2026-07-14)

Executado neste repositório com a simulação acima (dois projetos isolados, peers via IP da LAN `192.168.87.28`):

| Teste | Resultado |
|---|---|
| Malha completa: 6 bancos × 5 peers conectados através da fronteira | ✅ |
| Gossip de ordens propagado da máquina 1 para a 2 e vice-versa | ✅ |
| Bloco #1 com trade entre máquinas (comprador `bank_0` M1, vendedor `bank_5` M2) | ✅ |
| Votação manual dos gestores nas duas máquinas + quórum BFT | ✅ |
| Hash do bloco idêntico nas 6 réplicas | ✅ |
| Bloco #2 commitado com `bank_4` derrubado (tolerância a falha) | ✅ |
| `bank_4` reiniciado recuperou a cadeia via Postgres + chain sync remoto | ✅ |

## Notas de firewall

Cada nó bancário escuta em `0.0.0.0` na sua porta designada. O firewall do host
precisa aceitar TCP de entrada nas portas 9000–9005 vindo do IP da outra máquina.
As portas 8000–8005 (API/dashboard) só precisam ser liberadas se você quiser
acessar os dashboards a partir de outra máquina.

## Desenvolvimento em máquina única

Use o `docker-compose.yml` principal, que roda os 6 bancos num único host com
rede bridge interna do Docker.
