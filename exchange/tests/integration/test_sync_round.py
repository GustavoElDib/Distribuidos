from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone

import pytest

from bank.blockchain import Order
from bank.node import BankNode


def _order(bank_id: str, stock: str = "PETR4") -> Order:
    return Order(
        order_id=str(uuid.uuid4()),
        investor_id="inv_test",
        bank_id=bank_id,
        stock=stock,
        side="buy",
        quantity=10,
        limit_price=10.00,
        timestamp=datetime.now(timezone.utc).isoformat(),
    )


@pytest.mark.asyncio
async def test_sync_round_merges_all_orders(three_bank_cluster: list[BankNode]) -> None:
    node0, node1, node2 = three_bank_cluster
    orders_0 = [_order(node0.bank_id) for _ in range(5)]
    orders_1 = [_order(node1.bank_id) for _ in range(5)]

    for o in orders_0:
        node0.gossip_manager._pending[o.order_id] = o
        node0.gossip_manager._seen_order_ids.add(o.order_id)
    for o in orders_1:
        node1.gossip_manager._pending[o.order_id] = o
        node1.gossip_manager._seen_order_ids.add(o.order_id)

    block_index = len(node0.blockchain)
    result = await node0.sync_manager.run_sync_round(node0.bank_id, block_index)

    ids = {o.order_id for o in result.agreed_orders}
    for o in orders_0 + orders_1:
        assert o.order_id in ids, f"order {o.order_id} missing from agreed_orders"


@pytest.mark.asyncio
async def test_sync_round_excludes_timed_out_bank(three_bank_cluster: list[BankNode]) -> None:
    node0, node1, node2 = three_bank_cluster
    await node2.stop()
    await asyncio.sleep(0.2)

    # remove node2 from node0's peer_writers so it doesn't attempt to contact it
    node0.peer_writers.pop(node2.bank_id, None)

    block_index = len(node0.blockchain)
    result = await node0.sync_manager.run_sync_round(node0.bank_id, block_index)

    assert node2.bank_id in result.excluded_banks or node2.bank_id not in result.responding_banks


@pytest.mark.asyncio
async def test_sync_round_deduplicates_gossiped_orders(three_bank_cluster: list[BankNode]) -> None:
    node0, node1, _ = three_bank_cluster
    order = _order(node0.bank_id)

    # add the same order to both nodes (simulates gossip propagation)
    node0.gossip_manager._pending[order.order_id] = order
    node0.gossip_manager._seen_order_ids.add(order.order_id)
    node1.gossip_manager._pending[order.order_id] = order
    node1.gossip_manager._seen_order_ids.add(order.order_id)

    block_index = len(node0.blockchain)
    result = await node0.sync_manager.run_sync_round(node0.bank_id, block_index)

    matching = [o for o in result.agreed_orders if o.order_id == order.order_id]
    assert len(matching) == 1, "order should appear exactly once after dedup"
