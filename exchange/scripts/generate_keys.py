#!/usr/bin/env python3
"""Generate Ed25519 keypairs for all bank nodes and write them to keys/."""
from __future__ import annotations

import sys
from pathlib import Path

# allow running from repo root
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bank.crypto import generate_keypair, save_keypair

N_BANKS = 6


def main() -> None:
    keys_dir = Path(__file__).resolve().parents[1] / "keys"
    keys_dir.mkdir(exist_ok=True)

    for i in range(N_BANKS):
        bank_id = f"bank_{i}"
        priv, _ = generate_keypair()
        save_keypair(keys_dir, bank_id, priv)
        print(f"generated keys for {bank_id}")

    print(f"keys written to {keys_dir}")


if __name__ == "__main__":
    main()
