#!/usr/bin/env python3
"""
Roda um subconjunto dos bancos em MAIS DE UMA MAQUINA (rede local).

Diferente do run_local.py (que fixa tudo em 127.0.0.1), este script le um
"manifest" JSON com o IP real de cada banco na rede e so inicia, nesta
maquina, os bancos que voce indicar em --run.

Passos:
  1. Gere as chaves UMA vez (em qualquer maquina):
         python scripts/generate_keys.py
     Copie a pasta keys/ inteira (todos os .pub e .priv) para as outras
     maquinas -- todos os nos precisam enxergar as chaves publicas de
     todos os bancos.

  2. Copie scripts/nodes.example.json para scripts/nodes.json e preencha
     com os IPs reais de cada maquina (ipconfig / ip addr). O arquivo deve
     ser IDENTICO em todas as maquinas.

  3. Em cada maquina, rode apenas os bancos que ela vai hospedar, ex.:
         # Maquina A (hospeda bank_0 e bank_1)
         python scripts/run_distributed.py --manifest scripts/nodes.json --run bank_0,bank_1

         # Maquina B (hospeda bank_2 e bank_3)
         python scripts/run_distributed.py --manifest scripts/nodes.json --run bank_2,bank_3

  4. Libere as portas TCP/API no firewall de cada maquina (ver DOCUMENTACAO
     ou o README gerado por este script para o comando do Windows).
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]   # exchange/
DATA_DIR = ROOT / "data"
KEYS_DIR = ROOT / "keys"


def load_manifest(path: Path) -> dict[str, dict]:
    data = json.loads(path.read_text(encoding="utf-8"))
    return {n["id"]: n for n in data["nodes"]}


def bank_env(bank_id: str, nodes: dict[str, dict]) -> dict[str, str]:
    me = nodes[bank_id]
    db_path = DATA_DIR / f"{bank_id}.db"

    env = os.environ.copy()
    env.update({
        "BANK_ID":                  bank_id,
        "BANK_HOST":                "0.0.0.0",   # escuta em todas as interfaces locais
        "BANK_PORT":                str(me["port"]),
        "API_PORT":                 str(me["api_port"]),
        "DB_URL":                   f"sqlite:///{db_path}",
        "KEYS_DIR":                 str(KEYS_DIR),
        "AUCTION_INTERVAL_SECONDS": os.environ.get("AUCTION_INTERVAL_SECONDS", "120"),
        "BLOCK_INTERVAL_SECONDS":   "9999",
        "GOSSIP_FANOUT":            "3",
    })

    peer_idx = 0
    for other_id, other in nodes.items():
        if other_id == bank_id:
            continue
        env[f"PEER_{peer_idx}"] = f"{other_id}:{other['host']}:{other['port']}"
        peer_idx += 1

    return env


def main() -> None:
    parser = argparse.ArgumentParser(description="Run exchange nodes across multiple machines")
    parser.add_argument("--manifest", required=True,
                         help="JSON com TODOS os bancos da rede (id/host/port/api_port)")
    parser.add_argument("--run", required=True,
                         help="IDs dos bancos a rodar NESTA maquina, separados por virgula (ex: bank_0,bank_1)")
    parser.add_argument("--clean", action="store_true",
                         help="apaga os bancos SQLite locais (apenas os desta maquina) antes de iniciar")
    args = parser.parse_args()

    manifest_path = Path(args.manifest)
    if not manifest_path.exists():
        sys.exit(f"Manifest '{manifest_path}' nao encontrado. Copie nodes.example.json e edite os IPs.")

    nodes = load_manifest(manifest_path)
    run_ids = [b.strip() for b in args.run.split(",") if b.strip()]
    for bid in run_ids:
        if bid not in nodes:
            sys.exit(f"'{bid}' nao esta no manifest {manifest_path}. IDs disponiveis: {list(nodes)}")

    DATA_DIR.mkdir(exist_ok=True)

    if args.clean:
        for bid in run_ids:
            f = DATA_DIR / f"{bid}.db"
            if f.exists():
                f.unlink()
        print("Bancos SQLite locais apagados.")

    if not KEYS_DIR.exists() or not any(KEYS_DIR.glob("*.pub")):
        sys.exit(
            f"Pasta de chaves '{KEYS_DIR}' vazia ou inexistente.\n"
            "Gere as chaves em UMA maquina (python scripts/generate_keys.py) e copie a\n"
            "pasta keys/ inteira para todas as outras maquinas antes de rodar este script."
        )

    f = (len(nodes) - 1) // 3
    q = 2 * f + 1
    print(f"\nRede completa: {len(nodes)} bancos  |  BFT: f={f}, quorum={q}")
    print(f"Rodando {len(run_ids)} banco(s) NESTA maquina: {', '.join(run_ids)}\n")

    procs: list[subprocess.Popen] = []
    for bid in run_ids:
        env = bank_env(bid, nodes)
        proc = subprocess.Popen([sys.executable, "-m", "bank"], cwd=str(ROOT), env=env)
        procs.append(proc)
        me = nodes[bid]
        print(f"  {bid}  TCP=0.0.0.0:{me['port']} (anunciado como {me['host']}:{me['port']})  "
              f"API=http://{me['host']}:{me['api_port']}")
        time.sleep(0.3)

    print("\nAcesse o painel de qualquer maquina da rede pelo IP acima, ex.:")
    for bid in run_ids:
        me = nodes[bid]
        print(f"  http://{me['host']}:{me['api_port']}/gestor")

    print("\nPress Ctrl+C to stop all local nodes.\n")

    def _shutdown(sig, frame):
        print("\nShutting down...")
        for p in procs:
            p.terminate()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    for proc in procs:
        proc.wait()

    print("Bancos locais finalizados.")


if __name__ == "__main__":
    main()