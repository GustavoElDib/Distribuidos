from __future__ import annotations

import asyncio
import logging
import random
from typing import TYPE_CHECKING

from .blockchain import Order
from .messages import MessageType, OrderGossipPayload, build_message

if TYPE_CHECKING:
    from .node import BankNode

logger = logging.getLogger(__name__)


class GossipManager:
    def __init__(self, node: "BankNode") -> None:
        self._node = node
        self._seen_order_ids: set[str] = set()
        self._pending: dict[str, Order] = {}
        self._queue: asyncio.Queue[Order] = asyncio.Queue()
        self._full_broadcast = False

    def enable_full_broadcast(self) -> None:
        self._full_broadcast = True

    def disable_full_broadcast(self) -> None:
        self._full_broadcast = False

    async def broadcast_order(self, order: Order) -> None:
        if order.order_id in self._seen_order_ids:
            return
        self._seen_order_ids.add(order.order_id)
        self._pending[order.order_id] = order
        await self._queue.put(order)
        await self._forward(order)

    async def handle_incoming_gossip(self, message) -> None:  # type: ignore[type-arg]
        payload = OrderGossipPayload.from_dict(message.payload)
        order = Order.from_dict(payload.order)
        if order.order_id in self._seen_order_ids:
            return
        self._seen_order_ids.add(order.order_id)
        self._pending[order.order_id] = order
        await self._queue.put(order)
        await self._forward(order)

    async def _forward(self, order: Order) -> None:
        peers = list(self._node.peer_writers.keys())
        if not peers:
            return
        if self._full_broadcast:
            targets = peers
        else:
            k = self._node.config.gossip_fanout
            targets = random.sample(peers, min(k, len(peers)))

        msg = build_message(
            MessageType.ORDER_GOSSIP,
            self._node.config.this_bank.bank_id,
            OrderGossipPayload(order=order.to_dict()),
        )
        for peer_id in targets:
            writer = self._node.peer_writers.get(peer_id)
            if writer is not None:
                try:
                    writer.write(msg.encode())
                    await writer.drain()
                except Exception as exc:
                    logger.warning("gossip send to %s failed: %s", peer_id, exc)

    async def get_pending_orders(self) -> list[Order]:
        return list(self._pending.values())

    async def clear_pending_orders(self) -> None:
        self._pending.clear()
        while not self._queue.empty():
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
