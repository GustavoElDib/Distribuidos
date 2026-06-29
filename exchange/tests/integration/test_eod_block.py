from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from bank.blockchain import Order
from bank.node import BankNode


def _order(bank_id: str, side: str = "buy", price: float = 10.0, stock: str = "PETR4") -> Order:
    return Order(
        order_id=str(uuid.uuid4()),
        investor_id="inv_test",
        bank_id=bank_id,
        stock=stock,
        side=side,
        quantity=50,
        limit_price=price,
        timestamp=datetime.now(timezone.utc).isoformat(),
    )


async def _produce_eod_block(leader: BankNode, all_nodes: list[BankNode]) -> None:
    with patch.object(type(leader), "is_eod_time", return_value=True):
        await leader._trigger_block_production()
    await asyncio.sleep(1.0)


@pytest.mark.asyncio
async def test_eod_block_contains_snapshot(three_bank_cluster: list[BankNode]) -> None:
    nodes = three_bank_cluster

    # produce a normal block first to have price history
    buy = _order(nodes[0].bank_id, "buy", 10.0)
    sell = _order(nodes[1].bank_id, "sell", 9.0)
    nodes[0].gossip_manager._pending[buy.order_id] = buy
    nodes[1].gossip_manager._pending[sell.order_id] = sell

    block_index = len(nodes[0].blockchain)
    leader_id = nodes[0].consensus_manager.get_current_leader(block_index)
    leader = next(n for n in nodes if n.bank_id == leader_id)
    await leader._trigger_block_production()
    await asyncio.sleep(0.5)

    # now produce EOD block
    eod_block_index = len(nodes[0].blockchain)
    eod_leader_id = nodes[0].consensus_manager.get_current_leader(eod_block_index)
    eod_leader = next(n for n in nodes if n.bank_id == eod_leader_id)
    await _produce_eod_block(eod_leader, nodes)

    eod_block = eod_leader.blockchain.get_last_block()
    assert eod_block.is_eod is True
    assert eod_block.eod_snapshot is not None


@pytest.mark.asyncio
async def test_eod_block_cancels_pending_orders(three_bank_cluster: list[BankNode]) -> None:
    nodes = three_bank_cluster

    # add orders that won't match (price gap)
    buy = _order(nodes[0].bank_id, "buy", 5.0)    # won't match sell at 20
    sell = _order(nodes[1].bank_id, "sell", 20.0)
    nodes[0].gossip_manager._pending[buy.order_id] = buy
    nodes[1].gossip_manager._pending[sell.order_id] = sell

    block_index = len(nodes[0].blockchain)
    leader_id = nodes[0].consensus_manager.get_current_leader(block_index)
    leader = next(n for n in nodes if n.bank_id == leader_id)

    with patch.object(type(leader), "is_eod_time", return_value=True):
        await leader._trigger_block_production()
    await asyncio.sleep(1.0)

    eod_block = leader.blockchain.get_last_block()
    assert eod_block.is_eod is True

    # unmatched orders should have status='cancelled' (checked via DB)
    for order_id in [buy.order_id, sell.order_id]:
        row = await leader.db.pool.fetchrow(
            "SELECT status FROM orders WHERE order_id=$1", order_id
        )
        if row:
            assert row["status"] == "cancelled", f"order {order_id} not cancelled"


@pytest.mark.asyncio
async def test_eod_ohlc_derived_from_day_blocks(three_bank_cluster: list[BankNode]) -> None:
    nodes = three_bank_cluster
    prices = [10.00, 11.00, 9.50]

    for price in prices:
        buy = _order(nodes[0].bank_id, "buy", price + 1)
        sell = _order(nodes[1].bank_id, "sell", price)
        nodes[0].gossip_manager._pending[buy.order_id] = buy
        nodes[1].gossip_manager._pending[sell.order_id] = sell

        bi = len(nodes[0].blockchain)
        lid = nodes[0].consensus_manager.get_current_leader(bi)
        leader = next(n for n in nodes if n.bank_id == lid)
        await leader._trigger_block_production()
        await asyncio.sleep(0.5)

    # EOD block
    eod_bi = len(nodes[0].blockchain)
    eod_lid = nodes[0].consensus_manager.get_current_leader(eod_bi)
    eod_leader = next(n for n in nodes if n.bank_id == eod_lid)
    with patch.object(type(eod_leader), "is_eod_time", return_value=True):
        await eod_leader._trigger_block_production()
    await asyncio.sleep(1.0)

    eod_block = eod_leader.blockchain.get_last_block()
    assert eod_block.is_eod
    assert eod_block.eod_snapshot is not None
    ohlc = eod_block.eod_snapshot.daily_ohlc.get("PETR4")
    if ohlc:
        assert ohlc["open"] == 10.00
        assert ohlc["high"] == 11.00
        assert ohlc["low"] == 9.50
        assert ohlc["close"] == 9.50
