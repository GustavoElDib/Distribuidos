from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone

import pytest

from bank.blockchain import Order
from bank.node import BankNode


def _order(bank_id: str) -> Order:
    return Order(
        order_id=str(uuid.uuid4()),
        investor_id=f"inv_{uuid.uuid4().hex[:8]}",  # unico: o leilao exclui self-trade
        bank_id=bank_id,
        stock="PETR4",
        side="buy",
        quantity=10,
        limit_price=10.00,
        timestamp=datetime.now(timezone.utc).isoformat(),
    )


async def _produce_next_block(nodes: list[BankNode]) -> None:
    available = [n for n in nodes if not n._server is None or True]
    block_index = available[0].blockchain.get_last_block().index + 1
    leader_id = available[0].consensus_manager.get_current_leader(block_index)
    leaders = [n for n in available if n.bank_id == leader_id]
    if leaders:
        await leaders[0]._trigger_block_production()
    else:
        # líder da rodada está fora do ar — outro banco assume via force
        await available[0]._trigger_block_production(force=True)
    await asyncio.sleep(0.8)


@pytest.mark.asyncio
async def test_crashed_bank_syncs_chain_on_reconnect(three_bank_cluster: list[BankNode]) -> None:
    node0, node1, node2 = three_bank_cluster

    # produce 3 blocks with all banks up
    for _ in range(3):
        await _produce_next_block([node0, node1, node2])

    assert len(node0.blockchain) == 4  # genesis + 3

    # crash node2
    await node2.stop()
    node0.peer_writers.pop(node2.bank_id, None)
    node1.peer_writers.pop(node2.bank_id, None)

    # produce 2 more blocks without node2
    for _ in range(2):
        await _produce_next_block([node0, node1])

    assert len(node0.blockchain) == 6

    # restart node2 — it will reconnect and issue CHAIN_SYNC_REQUEST
    await node2.start()
    await asyncio.sleep(2.0)  # allow reconnect + sync

    assert len(node2.blockchain) == 6
    assert node2.blockchain.validate_chain()


@pytest.mark.asyncio
async def test_crashed_leader_triggers_next_leader(three_bank_cluster: list[BankNode]) -> None:
    nodes = three_bank_cluster
    block_index = len(nodes[0].blockchain)
    leader_id = nodes[0].consensus_manager.get_current_leader(block_index)
    crashed_leader = next(n for n in nodes if n.bank_id == leader_id)
    remaining = [n for n in nodes if n.bank_id != leader_id]

    await crashed_leader.stop()
    for node in remaining:
        node.peer_writers.pop(crashed_leader.bank_id, None)

    # next leader should step in (advance block_index by 1 due to skipped leader)
    next_leader_id = remaining[0].consensus_manager.get_current_leader(block_index + 1)
    next_leader = next((n for n in remaining if n.bank_id == next_leader_id), remaining[0])
    # o líder original caiu — o substituto produz via force
    await next_leader._trigger_block_production(force=True)
    await asyncio.sleep(1.0)

    # at least one remaining bank committed a block
    assert any(len(n.blockchain) > 1 for n in remaining)


@pytest.mark.asyncio
async def test_system_continues_with_minority_down(six_bank_cluster: list[BankNode]) -> None:
    nodes = six_bank_cluster

    # stop 2 nodes (minority)
    to_stop = nodes[:2]
    active = nodes[2:]
    for n in to_stop:
        await n.stop()
    for active_node in active:
        for stopped in to_stop:
            active_node.peer_writers.pop(stopped.bank_id, None)

    block_index = active[0].blockchain.get_last_block().index + 1
    leader_id = active[0].consensus_manager.get_current_leader(block_index)
    leaders = [n for n in active if n.bank_id == leader_id]
    if leaders:
        await leaders[0]._trigger_block_production()
    else:
        # líder da rodada está entre os que caíram — outro banco assume
        await active[0]._trigger_block_production(force=True)

    await asyncio.sleep(1.0)

    committed = [n for n in active if len(n.blockchain) > 1]
    assert len(committed) >= 3  # quorum = 4//2 + 1 = 3
