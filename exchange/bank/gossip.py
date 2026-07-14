from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from .blockchain import Order
from .messages import MessageType, OrderGossipPayload, build_message

if TYPE_CHECKING:
    from .node import BankNode

logger = logging.getLogger(__name__)


class GossipManager:
    """Flooding gossip: every received order is forwarded to ALL connected peers."""

    def __init__(self, node: "BankNode") -> None:
        self._node = node
        self._seen_order_ids: set[str] = set()
        self._pending: dict[str, Order] = {}
        self._queue: asyncio.Queue[Order] = asyncio.Queue()

    # kept for compatibility with sync.py calls — flooding is always active
    def enable_full_broadcast(self) -> None:
        pass

    def disable_full_broadcast(self) -> None:
        pass

    async def broadcast_order(self, order: Order) -> None:
        if order.order_id in self._seen_order_ids:
            return
        self._seen_order_ids.add(order.order_id)
        self._pending[order.order_id] = order
        await self._queue.put(order)
        await self._flood(order)

    async def handle_incoming_gossip(self, message) -> None:  # type: ignore[type-arg]
        payload = OrderGossipPayload.from_dict(message.payload)
        order = Order.from_dict(payload.order)
        if order.order_id in self._seen_order_ids:
            return
        self._seen_order_ids.add(order.order_id)
        self._pending[order.order_id] = order
        await self._queue.put(order)
        await self._flood(order)

    async def _flood(self, order: Order) -> None:
        """Forward order to every connected peer (flooding, not gossip fanout)."""
        peers = list(self._node.peer_writers.keys())
        if not peers:
            return
        msg = build_message(
            MessageType.ORDER_GOSSIP,
            self._node.config.this_bank.bank_id,
            OrderGossipPayload(order=order.to_dict()),
        )
        for peer_id in peers:
            writer = self._node.peer_writers.get(peer_id)
            if writer is not None:
                try:
                    writer.write(msg.encode())
                    await writer.drain()
                except Exception as exc:
                    logger.warning("flood to %s failed: %s", peer_id, exc)

    async def get_pending_orders(self) -> list[Order]:
        return list(self._pending.values())

    async def clear_pending_orders(self) -> None:
        self._pending.clear()
        while not self._queue.empty():
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
