from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone

import pytest
import pytest_asyncio

from bank.blockchain import Order
from bank.node import BankNode


def _make_order(bank_id: str = "bank_0") -> Order:
    return Order(
        order_id=str(uuid.uuid4()),
        investor_id=f"inv_{uuid.uuid4().hex[:8]}",  # unico: o leilao exclui self-trade
        bank_id=bank_id,
        stock="PETR4",
        side="buy",
        quantity=100,
        limit_price=10.00,
        timestamp=datetime.now(timezone.utc).isoformat(),
    )


async def _wait_for_order(node: BankNode, order_id: str, timeout: float = 5.0) -> bool:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        orders = await node.gossip_manager.get_pending_orders()
        if any(o.order_id == order_id for o in orders):
            return True
        await asyncio.sleep(0.1)
    return False


@pytest.mark.asyncio
async def test_order_reaches_all_peers(three_bank_cluster: list[BankNode]) -> None:
    order = _make_order(bank_id=three_bank_cluster[0].bank_id)
    await three_bank_cluster[0].submit_order(order)

    for node in three_bank_cluster:
        found = await _wait_for_order(node, order.order_id, timeout=5.0)
        assert found, f"order not found in {node.bank_id}"


@pytest.mark.asyncio
async def test_deduplication(three_bank_cluster: list[BankNode]) -> None:
    order = _make_order(bank_id=three_bank_cluster[0].bank_id)
    await three_bank_cluster[0].submit_order(order)
    await three_bank_cluster[0].submit_order(order)  # duplicate

    await asyncio.sleep(1.0)

    for node in three_bank_cluster:
        orders = await node.gossip_manager.get_pending_orders()
        matching = [o for o in orders if o.order_id == order.order_id]
        assert len(matching) <= 1, f"{node.bank_id} has duplicate order"


@pytest.mark.asyncio
async def test_gossip_with_one_peer_down(three_bank_cluster: list[BankNode]) -> None:
    node0, node1, node2 = three_bank_cluster
    await node2.stop()

    order = _make_order(bank_id=node0.bank_id)
    await node0.submit_order(order)

    found_1 = await _wait_for_order(node1, order.order_id, timeout=5.0)
    assert found_1, "bank_1 should have received the order"

    orders2 = await node2.gossip_manager.get_pending_orders()
    assert not any(o.order_id == order.order_id for o in orders2), \
        "bank_2 was down and should not have the order"
