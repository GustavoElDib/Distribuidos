from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from .blockchain import Order
from .messages import (
    MessageType,
    SyncOrdersPayload,
    SyncAckPayload,
    CloseWindowPayload,
    build_message,
)

if TYPE_CHECKING:
    from .node import BankNode

logger = logging.getLogger(__name__)


@dataclass
class SyncResult:
    agreed_orders: list[Order]
    responding_banks: list[str]
    excluded_banks: list[str]


class SyncManager:
    def __init__(self, node: "BankNode") -> None:
        self._node = node
        # futures keyed by bank_id, resolved when SYNC_ORDERS arrives
        self._pending_sync: dict[str, asyncio.Future[list[Order]]] = {}

    async def run_sync_round(self, leader_id: str, block_index: int) -> SyncResult:
        cfg = self._node.config
        peer_ids = list(self._node.peer_writers.keys())
        expected_banks = set(peer_ids)

        self._node.gossip_manager.enable_full_broadcast()

        # create futures for each expected peer BEFORE sending anything,
        # so responses arriving immediately after CLOSE_WINDOW are not lost
        loop = asyncio.get_event_loop()
        for peer_id in expected_banks:
            self._pending_sync[peer_id] = loop.create_future()

        # send CLOSE_WINDOW so peers know to sync their orders
        close_msg = build_message(
            MessageType.CLOSE_WINDOW,
            cfg.this_bank.bank_id,
            CloseWindowPayload(block_index=block_index),
        )
        await self._node.broadcast_to_all(close_msg)

        # broadcast our own pending orders to all peers
        own_orders = await self._node.gossip_manager.get_pending_orders()
        sync_msg = build_message(
            MessageType.SYNC_ORDERS,
            cfg.this_bank.bank_id,
            SyncOrdersPayload(
                orders=[o.to_dict() for o in own_orders],
                block_index=block_index,
            ),
        )
        await self._node.broadcast_to_all(sync_msg)

        # wait for SYNC_ORDERS from all peers with timeout
        done: dict[str, list[Order]] = {cfg.this_bank.bank_id: own_orders}
        responding: list[str] = [cfg.this_bank.bank_id]
        excluded: list[str] = []

        results = await asyncio.gather(
            *[self._wait_for_peer(peer_id, cfg.sync_timeout_seconds) for peer_id in expected_banks],
            return_exceptions=True,
        )

        for peer_id, result in zip(expected_banks, results):
            if isinstance(result, Exception):
                logger.warning("sync: %s timed out or errored: %s", peer_id, result)
                excluded.append(peer_id)
            else:
                done[peer_id] = result  # type: ignore[assignment]
                responding.append(peer_id)
                # send SYNC_ACK back
                ack = build_message(
                    MessageType.SYNC_ACK,
                    cfg.this_bank.bank_id,
                    SyncAckPayload(block_index=block_index, from_bank_id=cfg.this_bank.bank_id),
                )
                writer = self._node.peer_writers.get(peer_id)
                if writer:
                    try:
                        writer.write(ack.encode())
                        await writer.drain()
                    except Exception as exc:
                        logger.warning("ack send to %s failed: %s", peer_id, exc)

        # merge and deduplicate
        seen: set[str] = set()
        agreed: list[Order] = []
        for orders in done.values():
            for order in orders:
                if order.order_id not in seen:
                    seen.add(order.order_id)
                    agreed.append(order)

        # cleanup futures
        self._pending_sync.clear()

        return SyncResult(
            agreed_orders=agreed,
            responding_banks=responding,
            excluded_banks=excluded,
        )

    async def _wait_for_peer(self, peer_id: str, timeout: int) -> list[Order]:
        fut = self._pending_sync.get(peer_id)
        if fut is None:
            raise ValueError(f"no future for peer {peer_id}")
        return await asyncio.wait_for(fut, timeout=timeout)

    def handle_sync_orders(self, sender_id: str, orders: list[Order]) -> None:
        """Called by node._handle_message when SYNC_ORDERS arrives."""
        fut = self._pending_sync.get(sender_id)
        if fut is not None and not fut.done():
            fut.set_result(orders)

    def handle_sync_ack(self, sender_id: str) -> None:
        logger.debug("sync ack from %s", sender_id)
