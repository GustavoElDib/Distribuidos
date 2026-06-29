# Two-Machine Deployment

This guide explains how to run the 6-bank cluster split across two physical machines on the same LAN.

## Prerequisites

- Docker + Docker Compose installed on both machines
- Both machines on the same LAN (can ping each other)
- Port 9000–9002 open on machine 1, ports 9003–9005 open on machine 2

## Step 1 — Generate keypairs (once, on machine 1)

```bash
cd exchange
python scripts/generate_keys.py
```

Copy the generated `keys/` directory to the same path on machine 2:

```bash
scp -r keys/ user@<MACHINE2_IP>:~/exchange/keys/
```

## Step 2 — Start machine 1 (banks 0–2)

```bash
export MACHINE2_IP=192.168.1.20   # replace with actual IP
docker compose -f docker-compose.machine1.yml up --build -d
```

## Step 3 — Start machine 2 (banks 3–5)

```bash
export MACHINE1_IP=192.168.1.10   # replace with actual IP
docker compose -f docker-compose.machine2.yml up --build -d
```

## Step 4 — Seed test data (optional)

Run on either machine after both clusters are up:

```bash
DB_BASE_URL=postgresql://exchange:exchange@localhost/exchange python scripts/seed_db.py
```

## Firewall notes

Each bank node binds on `0.0.0.0` and listens on its assigned port.
Make sure the host firewall allows inbound TCP on ports 9000–9005 from the other machine's IP.

## Single-machine development

Use the main `docker-compose.yml` which runs all 6 banks on one host with an internal Docker bridge network.
