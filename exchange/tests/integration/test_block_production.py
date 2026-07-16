from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone

import pytest

from bank.blockchain import Order
from bank.crypto import verify_block
from bank.node import BankNode


def _order(bank_id: str, side: str = "buy", price: float = 10.0) -> Order:
    return Order(
        order_id=str(uuid.uuid4()),
        investor_id="inv_test",
        bank_id=bank_id,
        stock="PETR4",
        side=side,
        quantity=50,
        limit_price=price,
        timestamp=datetime.now(timezone.utc).isoformat(),
    )


async def _produce_block_on_leader(nodes: list[BankNode]) -> None:
    block_index = len(nodes[0].blockchain)
    leader_id = nodes[0].consensus_manager.get_current_leader(block_index)
    leader = next(n for n in nodes if n.bank_id == leader_id)
    await leader._trigger_block_production()
    await asyncio.sleep(0.5)


@pytest.mark.asyncio
async def test_leader_produces_valid_block(three_bank_cluster: list[BankNode]) -> None:
    nodes = three_bank_cluster
    buy = _order(nodes[0].bank_id, "buy", 10.0)
    sell = _order(nodes[1].bank_id, "sell", 9.0)
    nodes[0].gossip_manager._pending[buy.order_id] = buy
    nodes[1].gossip_manager._pending[sell.order_id] = sell

    await _produce_block_on_leader(nodes)

    block_index = 1
    leader_id = nodes[0].consensus_manager.get_current_leader(block_index)
    leader = next(n for n in nodes if n.bank_id == leader_id)
    block = leader.blockchain.get_block(1)

    assert block.index == 1
    assert block.producer_id == leader_id
    expected_hash = leader.blockchain.compute_block_hash(block)
    assert block.block_hash == expected_hash

    pub_key = leader.peer_keys[leader_id]
    assert verify_block(pub_key, block.block_hash, block.signature)


@pytest.mark.asyncio
async def test_block_appended_to_all_chains(three_bank_cluster: list[BankNode]) -> None:
    nodes = three_bank_cluster
    await _produce_block_on_leader(nodes)
    await asyncio.sleep(0.5)

    for node in nodes:
        assert len(node.blockchain) == 2, f"{node.bank_id} missing block"
        assert node.blockchain.validate_chain()


@pytest.mark.asyncio
async def test_block_contains_all_agreed_orders(three_bank_cluster: list[BankNode]) -> None:
    nodes = three_bank_cluster
    orders = [_order(nodes[i % len(nodes)].bank_id) for i in range(4)]
    for o in orders:
        n = next(node for node in nodes if node.bank_id == o.bank_id)
        n.gossip_manager._pending[o.order_id] = o

    await _produce_block_on_leader(nodes)

    leader_idx = nodes[0].consensus_manager.get_current_leader(1)
    leader = next(n for n in nodes if n.bank_id == leader_idx)
    block = leader.blockchain.get_block(1)
    block_order_ids = {o.order_id for o in block.orders}

    for o in orders:
        assert o.order_id in block_order_ids


@pytest.mark.asyncio
async def test_no_block_created_without_trades(three_bank_cluster: list[BankNode]) -> None:
    """Com min_trades_per_block=1, leilão sem casamento de ordens não gera bloco."""
    nodes = three_bank_cluster
    for node in nodes:
        node.config.min_trades_per_block = 1

    # apenas ordens de compra — nenhum trade possível
    only_buys = [_order(nodes[i % len(nodes)].bank_id, "buy", 10.0) for i in range(3)]
    for o in only_buys:
        n = next(node for node in nodes if node.bank_id == o.bank_id)
        n.gossip_manager._pending[o.order_id] = o

    await _produce_block_on_leader(nodes)

    for node in nodes:
        assert len(node.blockchain) == 1, f"{node.bank_id} criou bloco sem trades"

    # ordens pendentes são mantidas para o próximo leilão
    leader_id = nodes[0].consensus_manager.get_current_leader(1)
    leader = next(n for n in nodes if n.bank_id == leader_id)
    pending = await leader.gossip_manager.get_pending_orders()
    assert len(pending) >= 1

    # ao adicionar a contraparte de venda, o bloco é criado normalmente
    sell = _order(nodes[1].bank_id, "sell", 9.0)
    nodes[1].gossip_manager._pending[sell.order_id] = sell

    await _produce_block_on_leader(nodes)

    for node in nodes:
        assert len(node.blockchain) == 2, f"{node.bank_id} não criou bloco com trades"
    block = nodes[0].blockchain.get_block(1)
    assert len(block.trades) >= 1


@pytest.mark.asyncio
async def test_merkle_root_is_consistent(three_bank_cluster: list[BankNode]) -> None:
    nodes = three_bank_cluster
    buy = _order(nodes[0].bank_id, "buy", 10.0)
    sell = _order(nodes[1].bank_id, "sell", 9.0)
    nodes[0].gossip_manager._pending[buy.order_id] = buy
    nodes[1].gossip_manager._pending[sell.order_id] = sell

    await _produce_block_on_leader(nodes)

    block = nodes[0].blockchain.get_block(1)
    recomputed = nodes[0].blockchain.compute_merkle_root(block.trades)
    assert block.merkle_root == recomputed
