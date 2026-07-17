from __future__ import annotations

import asyncio
import os
import socket
import uuid
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import AsyncGenerator

import pytest
import pytest_asyncio

from bank.config import BankConfig, Config
from bank.crypto import generate_keypair, save_keypair
from bank.node import BankNode


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _make_cluster_config(
    n: int, keys_dir: Path, test_db_base_url: str
) -> list[Config]:
    bank_ids = [f"bank_{i}" for i in range(n)]
    ports = [_free_port() for _ in range(n)]

    bank_cfgs = [
        BankConfig(
            bank_id=bank_ids[i],
            host="127.0.0.1",
            port=ports[i],
            db_url=f"{test_db_base_url}_{bank_ids[i]}",
        )
        for i in range(n)
    ]

    configs: list[Config] = []
    for i in range(n):
        peers = [bank_cfgs[j] for j in range(n) if j != i]
        configs.append(
            Config(
                this_bank=bank_cfgs[i],
                peers=peers,
                keys_dir=keys_dir,
                # tight timeouts for tests
                sync_timeout_seconds=3,
                vote_timeout_seconds=3,
                # sem gestor humano nos testes: cai imediatamente no voto
                # automático (sem isso cada candidato espera 75s pelo popup)
                manager_vote_timeout_seconds=0,
                leader_timeout_seconds=5,
                heartbeat_interval_seconds=2,
                peer_timeout_seconds=8,
                block_interval_seconds=9999,  # never auto-trigger
                min_trades_per_block=0,  # muitos testes produzem blocos sem trades

            )
        )
    return configs


async def _spin_up_cluster(n: int) -> tuple[list[BankNode], TemporaryDirectory]:
    test_db_url = os.environ.get(
        "TEST_DB_URL", "postgresql://exchange:exchange@localhost/exchange_test"
    )
    # sufixo único por cluster: sem ele os bancos de teste acumulam blocos de
    # runs anteriores (a chain é restaurada do DB no start) e os testes que
    # esperam uma chain só com o genesis falham
    test_db_url = f"{test_db_url}_{uuid.uuid4().hex[:8]}"
    tmp = TemporaryDirectory()
    keys_dir = Path(tmp.name)

    configs = _make_cluster_config(n, keys_dir, test_db_url)

    # generate keypairs
    for cfg in configs:
        priv, _ = generate_keypair()
        save_keypair(keys_dir, cfg.this_bank.bank_id, priv)

    nodes = [BankNode(cfg.this_bank.bank_id, cfg) for cfg in configs]
    for node in nodes:
        await node.start()

    # allow time for peer connections
    await asyncio.sleep(0.5)
    return nodes, tmp


async def _tear_down_cluster(nodes: list[BankNode]) -> None:
    for node in nodes:
        try:
            await node.stop()
        except Exception:
            pass


@pytest_asyncio.fixture
async def three_bank_cluster() -> AsyncGenerator[list[BankNode], None]:
    nodes, tmp = await _spin_up_cluster(3)
    try:
        yield nodes
    finally:
        await _tear_down_cluster(nodes)
        tmp.cleanup()


@pytest_asyncio.fixture
async def six_bank_cluster() -> AsyncGenerator[list[BankNode], None]:
    nodes, tmp = await _spin_up_cluster(6)
    try:
        yield nodes
    finally:
        await _tear_down_cluster(nodes)
        tmp.cleanup()
