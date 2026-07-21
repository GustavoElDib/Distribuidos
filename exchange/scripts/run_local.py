from __future__ import annotations

import argparse
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]   # exchange/
DATA_DIR = ROOT / "data"
KEYS_DIR = ROOT / "keys"

BASE_PEER_PORT = 9000
BASE_API_PORT  = 8000


def bank_env(bank_idx: int, num_banks: int) -> dict[str, str]:
    bank_id   = f"bank_{bank_idx}"
    peer_port = BASE_PEER_PORT + bank_idx
    api_port  = BASE_API_PORT  + bank_idx
    db_path   = DATA_DIR / f"{bank_id}.db"

    env = os.environ.copy()
    env.update({
        "BANK_ID":                bank_id,
        "BANK_HOST":              "127.0.0.1",
        "BANK_PORT":              str(peer_port),
        "API_PORT":               str(api_port),
        "DB_URL":                 f"sqlite:///{db_path}",
        "KEYS_DIR":               str(KEYS_DIR),
        "AUCTION_INTERVAL_SECONDS": "120",  # 2 min for demo (prod default = 300s)
        "BLOCK_INTERVAL_SECONDS": "9999",   # disable legacy trigger
        "GOSSIP_FANOUT":          "3",
    })

    peer_idx = 0
    for other in range(num_banks):
        if other == bank_idx:
            continue
        env[f"PEER_{peer_idx}"] = f"bank_{other}:127.0.0.1:{BASE_PEER_PORT + other}"
        peer_idx += 1

    return env


def ensure_keys(num_banks: int) -> None:
    existing = list(KEYS_DIR.glob("*.priv"))
    if len(existing) >= num_banks:
        return
    print("Generating cryptographic keypairs...")
    subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "generate_keys.py")],
        cwd=str(ROOT),
        check=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run exchange nodes locally")
    parser.add_argument("--banks", type=int, default=4,
                        help="number of bank nodes (default: 4, min for BFT f=1)")
    parser.add_argument("--clean", action="store_true",
                        help="delete existing databases before starting")
    args = parser.parse_args()

    num_banks = max(args.banks, 1)

    if args.clean and DATA_DIR.exists():
        shutil.rmtree(DATA_DIR)
        print("Cleared existing databases.")

    DATA_DIR.mkdir(exist_ok=True)
    ensure_keys(num_banks)

    # BFT info
    f = (num_banks - 1) // 3
    q = 2 * f + 1
    print(f"\nStarting {num_banks} bank nodes  |  BFT: f={f}, quorum={q}/{num_banks}\n")

    # On Windows, Popen.terminate() calls TerminateProcess() — a hard kill that
    # the child never sees as a signal, so its `finally: await node.stop()`
    # never runs and the listening socket isn't closed before the process dies.
    # Starting each child in its own process group lets us send CTRL_BREAK_EVENT
    # instead, which Python does translate into a catchable KeyboardInterrupt/
    # SIGBREAK, giving the child a chance to shut down cleanly.
    popen_kwargs = {}
    if sys.platform == "win32":
        popen_kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP

    procs: list[subprocess.Popen] = []
    for i in range(num_banks):
        env = bank_env(i, num_banks)
        proc = subprocess.Popen(
            [sys.executable, "-m", "bank"],
            cwd=str(ROOT),
            env=env,
            **popen_kwargs,
        )
        procs.append(proc)
        print(f"  bank_{i}  TCP=:{BASE_PEER_PORT+i}  API=http://localhost:{BASE_API_PORT+i}")
        time.sleep(0.3)   # stagger startup slightly

    print(f"\nDashboards:")
    for i in range(num_banks):
        print(f"  http://localhost:{BASE_API_PORT+i}  (bank_{i})")


    GRACE_PERIOD_SECONDS = 10

    def _shutdown(sig, frame):
        print("\nShutting down...")
        for p in procs:
            if sys.platform == "win32":
                p.send_signal(signal.CTRL_BREAK_EVENT)
            else:
                p.terminate()

        deadline = time.monotonic() + GRACE_PERIOD_SECONDS
        for p in procs:
            remaining = max(0.0, deadline - time.monotonic())
            try:
                p.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                print(f"  process {p.pid} did not stop in time, killing it")
                p.kill()
                p.wait()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    for proc in procs:
        proc.wait()

    print("Todos os nós parados.")


if __name__ == "__main__":
    main()
