from __future__ import annotations

import asyncio
import logging
from collections import deque
from datetime import datetime, timezone
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
from .db import make_database
from .gossip import GossipManager
from .messages import (
    Message,
    MessageType,
    BlockVotePayload,
    BlockCommitPayload,
    BlockCandidatePayload,
    BlockRejectedPayload,
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
        self.db = make_database(config.this_bank.db_url)

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
        self._auction_window_opened: float = 0.0  # when the current window started

        self._block_production_lock = asyncio.Lock()
        self._leader_timeout_task: Optional[asyncio.Task] = None

        # Human-in-the-loop voting: candidate block awaiting this bank
        # manager's manual approve/reject decision (shown as popup in dashboard)
        self._pending_manager_vote: Optional[dict] = None
        self._manager_vote_future: Optional[asyncio.Future[bool]] = None

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

        self._last_block_time = asyncio.get_event_loop().time()
        self._auction_window_opened = self._last_block_time

        self._tasks = [
            asyncio.create_task(self._peer_connect_loop(), name="peer_connect"),
            asyncio.create_task(self._heartbeat_loop(), name="heartbeat"),
            asyncio.create_task(self._auction_timer_loop(), name="auction_timer"),
        ]

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

        elif mt == MessageType.BLOCK_REJECTED:
            payload = BlockRejectedPayload.from_dict(message.payload)
            self._handle_block_rejected(payload)

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
        from .crypto import sign_block

        # Automatic verification produces a RECOMMENDATION for the manager.
        auto_ok = await self.consensus_manager.verify_block(candidate)

        # Wait for the bank manager to manually approve/reject via the dashboard
        # popup. Falls back to the automatic recommendation on timeout so the
        # system still makes progress if no human is watching.
        accepted = await self._await_manager_decision(candidate, sender, auto_ok)

        vote_data = f"{candidate.index}:{candidate.block_hash}:{accepted}"
        vote_sig = sign_block(self.private_key, vote_data)
        vote = build_message(
            MessageType.BLOCK_VOTE,
            self.bank_id,
            BlockVotePayload(
                block_index=candidate.index,
                block_hash=candidate.block_hash,
                accepted=accepted,
                reason="" if accepted else "rejeitado pelo gestor",
                signature=vote_sig,
            ),
        )
        writer = self.peer_writers.get(sender)
        if writer:
            try:
                writer.write(vote.encode())
                await writer.drain()
            except Exception as exc:
                logger.warning("vote send to %s failed: %s", sender, exc)

    async def _await_manager_decision(
        self, candidate: Block, sender: str, auto_ok: bool
    ) -> bool:
        """Expose the candidate to the dashboard and wait for the manager's click."""
        loop = asyncio.get_event_loop()
        fut: asyncio.Future[bool] = loop.create_future()
        self._manager_vote_future = fut
        self._pending_manager_vote = {
            "block_index": candidate.index,
            "block_hash": candidate.block_hash,
            "producer_id": candidate.producer_id,
            "orders_count": len(candidate.orders),
            "trades_count": len(candidate.trades),
            "trades": [t.to_dict() for t in candidate.trades],
            "clearing_prices": candidate.clearing_prices,
            "auto_recommendation": auto_ok,
            "is_eod": candidate.is_eod,
            "received_at": datetime.now(timezone.utc).isoformat(),
            "deadline_seconds": self.config.manager_vote_timeout_seconds,
        }
        logger.info(
            "block %d from %s awaiting manager decision (auto recommends %s)",
            candidate.index, sender, "ACCEPT" if auto_ok else "REJECT",
        )
        try:
            decision = await asyncio.wait_for(
                fut, timeout=self.config.manager_vote_timeout_seconds
            )
            logger.info("manager voted %s on block %d",
                        "ACCEPT" if decision else "REJECT", candidate.index)
        except asyncio.TimeoutError:
            decision = auto_ok
            logger.info(
                "manager did not vote on block %d in time; using auto recommendation=%s",
                candidate.index, auto_ok,
            )
        finally:
            self._pending_manager_vote = None
            self._manager_vote_future = None
        return decision

    def submit_manager_vote(self, approve: bool) -> bool:
        """Called by the API when the manager clicks approve/reject in the popup."""
        fut = self._manager_vote_future
        if fut is not None and not fut.done():
            fut.set_result(approve)
            return True
        return False

    def get_pending_manager_vote(self) -> Optional[dict]:
        return self._pending_manager_vote

    async def _handle_block_commit(self, block: Block) -> None:
        try:
            await self.consensus_manager.commit_block(block)
            self._record_block_produced(len(block.orders))
        except ValueError as exc:
            logger.error("block commit rejected: %s", exc)

    def _handle_block_rejected(self, payload: BlockRejectedPayload) -> None:
        """A leader announced its block was rejected — rotate to the new leader."""
        # Only relevant if we're still at the same height (no block committed).
        if payload.block_index != len(self.blockchain):
            return
        self.consensus_manager.set_round(payload.round)
        self._auction_window_opened = asyncio.get_event_loop().time()
        logger.info(
            "block %d rejected by managers; round=%d, new leader=%s (fresh auction)",
            payload.block_index,
            payload.round,
            self.consensus_manager.get_current_leader(len(self.blockchain)),
        )

    # ------------------------------------------------------------------
    # Block production (leader path)
    # ------------------------------------------------------------------

    async def _trigger_block_production(self, force: bool = False) -> None:
        async with self._block_production_lock:
            block_index = len(self.blockchain)
            leader_id = self.consensus_manager.get_current_leader(block_index)
            if leader_id != self.bank_id and not force:
                return

            logger.info("bank %s is leader for block %d", self.bank_id, block_index)

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
                # Block rejected by the managers' votes. Do NOT re-propose the
                # same block. Instead rotate leadership to the next bank and let
                # a fresh auction happen. Pending orders are kept (not cleared).
                new_round = self.consensus_manager.advance_round()
                logger.warning(
                    "block %d REJECTED (accept=%d reject=%d) — rotating to round %d, "
                    "new leader=%s",
                    block_index,
                    vote_result.accept_count,
                    vote_result.reject_count,
                    new_round,
                    self.consensus_manager.get_current_leader(block_index),
                )
                rejected_msg = build_message(
                    MessageType.BLOCK_REJECTED,
                    self.bank_id,
                    BlockRejectedPayload(block_index=block_index, round=new_round),
                )
                await self.broadcast_to_all(rejected_msg)
                # Start a fresh auction window so the next leader runs a new leilão.
                self._auction_window_opened = asyncio.get_event_loop().time()

    def _record_block_produced(self, order_count: int) -> None:
        self._recent_block_counts.append(order_count)
        now = asyncio.get_event_loop().time()
        self._last_block_time = now
        self._auction_window_opened = now  # reset window after each block

    def get_auction_status(self) -> dict:
        """Return auction timer info for the dashboard."""
        cfg = self.config
        now = asyncio.get_event_loop().time()
        interval = cfg.auction_interval_seconds
        elapsed = now - self._auction_window_opened
        remaining = max(0.0, interval - elapsed)
        block_index = len(self.blockchain)
        leader_id = self.consensus_manager.get_current_leader(block_index)
        is_leader = leader_id == self.bank_id
        return {
            "auction_interval_seconds": interval,
            "elapsed_seconds": round(elapsed, 1),
            "remaining_seconds": round(remaining, 1),
            "current_leader": leader_id,
            "is_leader": is_leader,
            "next_block_index": block_index,
            "consensus_round": self.consensus_manager.current_round,
        }

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

    async def _auction_timer_loop(self) -> None:
        """Leader-controlled auction timer.

        The LEADER is responsible for closing the order window and producing
        a block every `auction_interval_seconds`. Non-leaders do nothing here —
        they rely on BLOCK_COMMIT messages to stay in sync.
        """
        cfg = self.config
        while True:
            await asyncio.sleep(5)  # check every 5s so countdown is responsive

            block_index = len(self.blockchain)
            leader_id = self.consensus_manager.get_current_leader(block_index)

            if leader_id != self.bank_id:
                continue  # not the leader for this round — do nothing

            now = asyncio.get_event_loop().time()
            elapsed = now - self._auction_window_opened

            if elapsed >= cfg.auction_interval_seconds:
                if not self._block_production_lock.locked():
                    logger.info(
                        "auction timer fired for block %d (elapsed=%.0fs)",
                        block_index, elapsed,
                    )
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
