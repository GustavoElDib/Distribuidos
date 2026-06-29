from __future__ import annotations

import asyncio
import logging
from collections import deque
from datetime import datetime, timezone
from statistics import mean
from typing import Optional

from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)

from .auction import run_call_auction
from .blockchain import Block, BlockChain, Order
from .config import Config
from .consensus import ConsensusManager
from .crypto import load_private_key, load_peer_keys
from .db import Database
from .gossip import GossipManager
from .messages import (
    Message,
    MessageType,
    BlockVotePayload,
    BlockCommitPayload,
    BlockCandidatePayload,
    ChainSyncRequestPayload,
    ChainSyncResponsePayload,
    CloseWindowPayload,
    HeartbeatPayload,
    SyncOrdersPayload,
    SyncAckPayload,
    build_message,
)
from .sync import SyncManager

logger = logging.getLogger(__name__)


class BankNode:
    def __init__(self, bank_id: str, config: Config) -> None:
        self.bank_id = bank_id
        self.config = config

        self.blockchain: BlockChain = BlockChain()
        self.db: Database = Database(config.this_bank.db_url)

        self.gossip_manager: GossipManager = GossipManager(self)
        self.sync_manager: SyncManager = SyncManager(self)
        self.consensus_manager: ConsensusManager = ConsensusManager(self)

        # peer_id -> StreamWriter for outbound connections
        self.peer_writers: dict[str, asyncio.StreamWriter] = {}
        # peer_id -> last heartbeat timestamp
        self._peer_last_seen: dict[str, float] = {}

        self.private_key: Ed25519PrivateKey = None  # type: ignore[assignment]
        self.peer_keys: dict[str, Ed25519PublicKey] = {}

        self._server: Optional[asyncio.Server] = None
        self._tasks: list[asyncio.Task] = []

        # rolling window of recent block order counts
        self._recent_block_counts: deque[int] = deque(maxlen=config.rolling_window_size)
        self._last_block_time: float = 0.0

        self._block_production_lock = asyncio.Lock()
        self._leader_timeout_task: Optional[asyncio.Task] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        self.private_key = load_private_key(self.config.keys_dir, self.bank_id)
        self.peer_keys = load_peer_keys(self.config.keys_dir)
        # include own public key in peer_keys
        if self.bank_id not in self.peer_keys:
            self.peer_keys[self.bank_id] = self.private_key.public_key()

        all_bank_ids = [self.bank_id] + [p.bank_id for p in self.config.peers]
        self.consensus_manager.initialize_bank_ids(all_bank_ids)

        await self.db.initialize()

        cfg = self.config.this_bank
        self._server = await asyncio.start_server(
            self._handle_connection,
            host=cfg.host,
            port=cfg.port,
        )
        logger.info("bank %s listening on %s:%d", self.bank_id, cfg.host, cfg.port)

        self._tasks = [
            asyncio.create_task(self._peer_connect_loop(), name="peer_connect"),
            asyncio.create_task(self._heartbeat_loop(), name="heartbeat"),
            asyncio.create_task(self._block_trigger_loop(), name="block_trigger"),
        ]
        self._last_block_time = asyncio.get_event_loop().time()

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        await self.db.close()
        logger.info("bank %s stopped", self.bank_id)

    # Phase 2: called by FastAPI
    async def submit_order(self, order: Order) -> None:
        await self.gossip_manager.broadcast_order(order)

    # ------------------------------------------------------------------
    # TCP server
    # ------------------------------------------------------------------

    async def _handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                try:
                    msg = Message.from_json(line.decode("utf-8").strip())
                    await self._handle_message(msg, writer)
                except Exception as exc:
                    logger.warning("message parse error: %s", exc)
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.warning("connection closed: %s", exc)
        finally:
            writer.close()

    async def _handle_message(
        self, message: Message, writer: asyncio.StreamWriter
    ) -> None:
        mt = message.msg_type
        sender = message.sender_id

        if mt == MessageType.ORDER_GOSSIP:
            await self.gossip_manager.handle_incoming_gossip(message)

        elif mt == MessageType.CLOSE_WINDOW:
            payload = CloseWindowPayload.from_dict(message.payload)
            asyncio.create_task(self._handle_close_window(payload.block_index, sender))

        elif mt == MessageType.SYNC_ORDERS:
            payload = SyncOrdersPayload.from_dict(message.payload)
            orders = [Order.from_dict(o) for o in payload.orders]
            self.sync_manager.handle_sync_orders(sender, orders)

        elif mt == MessageType.SYNC_ACK:
            self.sync_manager.handle_sync_ack(sender)

        elif mt == MessageType.BLOCK_CANDIDATE:
            payload = BlockCandidatePayload.from_dict(message.payload)
            candidate = Block.from_dict(payload.block)
            asyncio.create_task(self._handle_block_candidate(candidate, sender))

        elif mt == MessageType.BLOCK_VOTE:
            payload = BlockVotePayload.from_dict(message.payload)
            self.consensus_manager.handle_vote(sender, payload)

        elif mt == MessageType.BLOCK_COMMIT:
            payload = BlockCommitPayload.from_dict(message.payload)
            block = Block.from_dict(payload.block)
            asyncio.create_task(self._handle_block_commit(block))

        elif mt == MessageType.CHAIN_SYNC_REQUEST:
            payload = ChainSyncRequestPayload.from_dict(message.payload)
            await self._handle_chain_sync(payload.from_index, writer)

        elif mt == MessageType.CHAIN_SYNC_RESPONSE:
            payload = ChainSyncResponsePayload.from_dict(message.payload)
            asyncio.create_task(self._apply_chain_sync(payload.blocks))

        elif mt == MessageType.HEARTBEAT:
            payload = HeartbeatPayload.from_dict(message.payload)
            self._peer_last_seen[sender] = asyncio.get_event_loop().time()
            logger.debug("heartbeat from %s (chain_length=%d)", sender, payload.chain_length)

    # ------------------------------------------------------------------
    # Block flow handlers
    # ------------------------------------------------------------------

    async def _handle_close_window(self, block_index: int, leader_id: str) -> None:
        """Non-leader: respond to close window by sending our sync orders."""
        own_orders = await self.gossip_manager.get_pending_orders()
        from .messages import SyncOrdersPayload
        sync_msg = build_message(
            MessageType.SYNC_ORDERS,
            self.bank_id,
            SyncOrdersPayload(
                orders=[o.to_dict() for o in own_orders],
                block_index=block_index,
            ),
        )
        await self.broadcast_to_all(sync_msg)

        ack = build_message(
            MessageType.SYNC_ACK,
            self.bank_id,
            SyncAckPayload(block_index=block_index, from_bank_id=self.bank_id),
        )
        writer = self.peer_writers.get(leader_id)
        if writer:
            try:
                writer.write(ack.encode())
                await writer.drain()
            except Exception as exc:
                logger.warning("ack to leader %s failed: %s", leader_id, exc)

    async def _handle_block_candidate(self, candidate: Block, sender: str) -> None:
        accepted = await self.consensus_manager.verify_block(candidate)
        vote = build_message(
            MessageType.BLOCK_VOTE,
            self.bank_id,
            BlockVotePayload(
                block_index=candidate.index,
                block_hash=candidate.block_hash,
                accepted=accepted,
                reason="" if accepted else "auction mismatch",
            ),
        )
        writer = self.peer_writers.get(sender)
        if writer:
            try:
                writer.write(vote.encode())
                await writer.drain()
            except Exception as exc:
                logger.warning("vote send to %s failed: %s", sender, exc)

    async def _handle_block_commit(self, block: Block) -> None:
        try:
            await self.consensus_manager.commit_block(block)
            self._record_block_produced(len(block.orders))
        except ValueError as exc:
            logger.error("block commit rejected: %s", exc)

    # ------------------------------------------------------------------
    # Block production (leader path)
    # ------------------------------------------------------------------

    async def _trigger_block_production(self) -> None:
        async with self._block_production_lock:
            block_index = len(self.blockchain)
            leader_id = self.consensus_manager.get_current_leader(block_index)
            if leader_id != self.bank_id:
                return

            logger.info("bank %s is leader for block %d", self.bank_id, block_index)

            close_msg = build_message(
                MessageType.CLOSE_WINDOW,
                self.bank_id,
                CloseWindowPayload(block_index=block_index),
            )
            await self.broadcast_to_all(close_msg)

            sync_result = await self.sync_manager.run_sync_round(self.bank_id, block_index)
            block = await self.consensus_manager.produce_block(sync_result, block_index)
            vote_result = await self.consensus_manager.run_voting_round(block)

            if vote_result.accepted:
                commit_msg = build_message(
                    MessageType.BLOCK_COMMIT,
                    self.bank_id,
                    BlockCommitPayload(block=block.to_dict()),
                )
                await self.broadcast_to_all(commit_msg)
                await self.consensus_manager.commit_block(block)
                self._record_block_produced(len(block.orders))
            else:
                logger.warning(
                    "block %d rejected: accept=%d reject=%d",
                    block_index,
                    vote_result.accept_count,
                    vote_result.reject_count,
                )

    def _record_block_produced(self, order_count: int) -> None:
        self._recent_block_counts.append(order_count)
        self._last_block_time = asyncio.get_event_loop().time()

    # ------------------------------------------------------------------
    # Background loops
    # ------------------------------------------------------------------

    async def _peer_connect_loop(self) -> None:
        while True:
            for peer in self.config.peers:
                if peer.bank_id not in self.peer_writers:
                    try:
                        _, writer = await asyncio.open_connection(peer.host, peer.port)
                        self.peer_writers[peer.bank_id] = writer
                        logger.info("connected to peer %s", peer.bank_id)
                        # request chain sync
                        last_index = len(self.blockchain) - 1
                        sync_req = build_message(
                            MessageType.CHAIN_SYNC_REQUEST,
                            self.bank_id,
                            ChainSyncRequestPayload(from_index=last_index),
                        )
                        writer.write(sync_req.encode())
                        await writer.drain()
                    except Exception as exc:
                        logger.debug("could not connect to %s: %s", peer.bank_id, exc)
            await asyncio.sleep(5)

    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(self.config.heartbeat_interval_seconds)
            hb = build_message(
                MessageType.HEARTBEAT,
                self.bank_id,
                HeartbeatPayload(chain_length=len(self.blockchain)),
            )
            await self.broadcast_to_all(hb)
            # evict stale peers
            now = asyncio.get_event_loop().time()
            stale = [
                pid
                for pid, t in self._peer_last_seen.items()
                if now - t > self.config.peer_timeout_seconds
            ]
            for pid in stale:
                logger.warning("peer %s suspected crashed (no heartbeat)", pid)
                writer = self.peer_writers.pop(pid, None)
                if writer:
                    try:
                        writer.close()
                    except Exception:
                        pass

    async def _block_trigger_loop(self) -> None:
        while True:
            await asyncio.sleep(10)
            pending_orders = await self.gossip_manager.get_pending_orders()
            pending_count = len(pending_orders)
            now = asyncio.get_event_loop().time()
            time_since_last = now - self._last_block_time

            if self._recent_block_counts:
                rolling_avg = mean(self._recent_block_counts)
                volume_trigger = pending_count > rolling_avg * (
                    1.0 + self.config.volume_trigger_pct
                )
            else:
                volume_trigger = pending_count > 0

            time_trigger = time_since_last > self.config.block_interval_seconds

            if volume_trigger or time_trigger:
                asyncio.create_task(self._trigger_block_production())

    # ------------------------------------------------------------------
    # Chain sync
    # ------------------------------------------------------------------

    async def _handle_chain_sync(
        self, from_index: int, writer: asyncio.StreamWriter
    ) -> None:
        blocks = self.blockchain.get_blocks_from(from_index)
        resp = build_message(
            MessageType.CHAIN_SYNC_RESPONSE,
            self.bank_id,
            ChainSyncResponsePayload(blocks=[b.to_dict() for b in blocks]),
        )
        try:
            writer.write(resp.encode())
            await writer.drain()
        except Exception as exc:
            logger.warning("chain sync response failed: %s", exc)

    async def _apply_chain_sync(self, block_dicts: list[dict]) -> None:
        for d in block_dicts:
            block = Block.from_dict(d)
            if block.index <= self.blockchain.get_last_block().index:
                continue
            try:
                self.blockchain.append(block)
                await self.db.persist_block(block)
            except ValueError as exc:
                logger.error("chain sync block %d rejected: %s", block.index, exc)
                break
        logger.info(
            "chain sync complete, chain length=%d", len(self.blockchain)
        )

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    def is_eod_time(self) -> bool:
        now = datetime.now(timezone.utc).time()
        return now >= self.config.market_close

    async def broadcast_to_all(self, message: Message) -> None:
        data = message.encode()
        for peer_id, writer in list(self.peer_writers.items()):
            try:
                writer.write(data)
                await writer.drain()
            except Exception as exc:
                logger.warning("broadcast to %s failed: %s", peer_id, exc)
                self.peer_writers.pop(peer_id, None)
