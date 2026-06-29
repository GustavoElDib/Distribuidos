from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest

from bank.blockchain import Order
from bank.node import BankNode


def _order(bank_id: str) -> Order:
    return Order(
        order_id=str(uuid.uuid4()),
        investor_id="inv_test",
        bank_id=bank_id,
        stock="PETR4",
        side="buy",
        quantity=10,
        limit_price=10.00,
        timestamp=datetime.now(timezone.utc).isoformat(),
    )


@pytest.mark.asyncio
async def test_all_banks_accept_valid_block(three_bank_cluster: list[BankNode]) -> None:
    nodes = three_bank_cluster
    block_index = len(nodes[0].blockchain)
    leader_id = nodes[0].consensus_manager.get_current_leader(block_index)
    leader = next(n for n in nodes if n.bank_id == leader_id)

    await leader._trigger_block_production()
    await asyncio.sleep(1.0)

    # all chains should have grown
    for node in nodes:
        assert len(node.blockchain) == 2


@pytest.mark.asyncio
async def test_quorum_with_one_voter_down(three_bank_cluster: list[BankNode]) -> None:
    nodes = three_bank_cluster
    node0, node1, node2 = nodes

    block_index = len(node0.blockchain)
    leader_id = node0.consensus_manager.get_current_leader(block_index)
    leader = next(n for n in nodes if n.bank_id == leader_id)

    # stop a non-leader node
    non_leader = next(n for n in nodes if n.bank_id != leader_id)
    await non_leader.stop()
    leader.peer_writers.pop(non_leader.bank_id, None)

    await leader._trigger_block_production()
    await asyncio.sleep(1.0)

    assert len(leader.blockchain) == 2


@pytest.mark.asyncio
async def test_block_rejected_on_auction_mismatch(three_bank_cluster: list[BankNode]) -> None:
    nodes = three_bank_cluster
    block_index = len(nodes[0].blockchain)
    leader_id = nodes[0].consensus_manager.get_current_leader(block_index)
    leader = next(n for n in nodes if n.bank_id == leader_id)
    non_leaders = [n for n in nodes if n.bank_id != leader_id]

    # patch verify_block on all non-leaders to return False
    for node in non_leaders:
        node.consensus_manager.verify_block = AsyncMock(return_value=False)  # type: ignore[method-assign]

    await leader._trigger_block_production()
    await asyncio.sleep(1.0)

    # block should not have been committed (chain stays at genesis)
    assert len(leader.blockchain) == 1


@pytest.mark.asyncio
async def test_round_robin_leader_rotation(six_bank_cluster: list[BankNode]) -> None:
    nodes = six_bank_cluster
    seen_producers = []

    for _ in range(6):
        block_index = len(nodes[0].blockchain)
        leader_id = nodes[0].consensus_manager.get_current_leader(block_index)
        leader = next(n for n in nodes if n.bank_id == leader_id)
        await leader._trigger_block_production()
        await asyncio.sleep(0.5)
        block = nodes[0].blockchain.get_block(block_index)
        seen_producers.append(block.producer_id)

    # every bank_id must appear exactly once across the 6 blocks
    assert set(seen_producers) == {n.bank_id for n in nodes}
