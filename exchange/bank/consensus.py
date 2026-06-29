from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from .auction import run_call_auction
from .blockchain import Block, Order, Trade, EodSnapshot
from .messages import (
    MessageType,
    BlockCandidatePayload,
    BlockVotePayload,
    BlockCommitPayload,
    build_message,
)

if TYPE_CHECKING:
    from .node import BankNode

logger = logging.getLogger(__name__)


@dataclass
class VoteResult:
    accepted: bool
    accept_count: int
    reject_count: int
    total_responding: int


class ConsensusManager:
    def __init__(self, node: "BankNode") -> None:
        self._node = node
        self._vote_futures: dict[str, asyncio.Future[BlockVotePayload]] = {}
        self._sorted_bank_ids: list[str] = []

    def initialize_bank_ids(self, all_bank_ids: list[str]) -> None:
        self._sorted_bank_ids = sorted(all_bank_ids)

    def get_current_leader(self, block_index: int) -> str:
        return self._sorted_bank_ids[block_index % len(self._sorted_bank_ids)]

    async def produce_block(self, sync_result, block_index: int) -> Block:  # type: ignore[type-arg]
        from .sync import SyncResult
        assert isinstance(sync_result, SyncResult)

        cfg = self._node.config
        chain = self._node.blockchain
        last = chain.get_last_block()

        last_prices = last.clearing_prices

        auction_result = run_call_auction(
            orders=sync_result.agreed_orders,
            block_index=block_index,
            last_clearing_prices=last_prices,
        )

        is_eod = self._node.is_eod_time()
        eod_snapshot: EodSnapshot | None = None
        if is_eod:
            eod_snapshot = await self._node.db.build_eod_snapshot()

        merkle_root = chain.compute_merkle_root(auction_result.trades)

        block = Block(
            index=block_index,
            timestamp=datetime.now(timezone.utc).isoformat(),
            previous_hash=last.block_hash,
            producer_id=cfg.this_bank.bank_id,
            orders=sync_result.agreed_orders,
            trades=auction_result.trades,
            clearing_prices=auction_result.clearing_prices,
            merkle_root=merkle_root,
            is_eod=is_eod,
            eod_snapshot=eod_snapshot,
            block_hash="",
            signature="",
        )
        block.block_hash = chain.compute_block_hash(block)
        from .crypto import sign_block
        block.signature = sign_block(self._node.private_key, block.block_hash)

        return block

    async def verify_block(self, candidate: Block) -> bool:
        chain = self._node.blockchain
        last = chain.get_last_block()

        if candidate.previous_hash != last.block_hash:
            logger.warning(
                "verify: previous_hash mismatch on block %d", candidate.index
            )
            return False

        expected_hash = chain.compute_block_hash(candidate)
        if candidate.block_hash != expected_hash:
            logger.warning("verify: block_hash invalid on block %d", candidate.index)
            return False

        expected_merkle = chain.compute_merkle_root(candidate.trades)
        if candidate.merkle_root != expected_merkle:
            logger.warning("verify: merkle_root mismatch on block %d", candidate.index)
            return False

        from .crypto import verify_block, load_peer_keys
        peer_keys = self._node.peer_keys
        producer_key = peer_keys.get(candidate.producer_id)
        if producer_key is None:
            logger.warning("verify: unknown producer %s", candidate.producer_id)
            return False
        if not verify_block(producer_key, candidate.block_hash, candidate.signature):
            logger.warning("verify: signature invalid on block %d", candidate.index)
            return False

        last_prices = last.clearing_prices
        local_result = run_call_auction(
            orders=candidate.orders,
            block_index=candidate.index,
            last_clearing_prices=last_prices,
        )

        candidate_trade_ids = {t.trade_id for t in candidate.trades}
        local_trade_ids = {t.trade_id for t in local_result.trades}

        # compare by content (sorted canonical tuples), not UUIDs
        def _trade_key(t: Trade) -> tuple:
            return (t.stock, t.buyer_order_id, t.seller_order_id, t.quantity, t.price)

        candidate_keys = sorted(_trade_key(t) for t in candidate.trades)
        local_keys = sorted(_trade_key(t) for t in local_result.trades)

        if candidate_keys != local_keys:
            logger.warning(
                "verify: auction mismatch on block %d: candidate=%s local=%s",
                candidate.index, candidate_keys, local_keys,
            )
            return False

        if candidate.clearing_prices != local_result.clearing_prices:
            logger.warning(
                "verify: clearing_prices mismatch on block %d", candidate.index
            )
            return False

        return True

    async def run_voting_round(self, candidate: Block) -> VoteResult:
        cfg = self._node.config
        peer_ids = list(self._node.peer_writers.keys())
        loop = asyncio.get_event_loop()

        for peer_id in peer_ids:
            self._vote_futures[peer_id] = loop.create_future()

        msg = build_message(
            MessageType.BLOCK_CANDIDATE,
            cfg.this_bank.bank_id,
            BlockCandidatePayload(block=candidate.to_dict()),
        )
        await self._node.broadcast_to_all(msg)

        results = await asyncio.gather(
            *[self._wait_for_vote(pid, cfg.vote_timeout_seconds) for pid in peer_ids],
            return_exceptions=True,
        )

        accept = 0
        reject = 0
        responding = 0
        for result in results:
            if isinstance(result, Exception):
                logger.warning("vote: peer timed out")
                continue
            responding += 1
            if result.accepted:
                accept += 1
            else:
                reject += 1

        self._vote_futures.clear()

        quorum = (responding // 2) + 1
        accepted = accept >= quorum

        return VoteResult(
            accepted=accepted,
            accept_count=accept,
            reject_count=reject,
            total_responding=responding,
        )

    async def _wait_for_vote(self, peer_id: str, timeout: int) -> BlockVotePayload:
        fut = self._vote_futures.get(peer_id)
        if fut is None:
            raise ValueError(f"no vote future for {peer_id}")
        return await asyncio.wait_for(fut, timeout=timeout)

    def handle_vote(self, sender_id: str, payload: BlockVotePayload) -> None:
        fut = self._vote_futures.get(sender_id)
        if fut is not None and not fut.done():
            fut.set_result(payload)

    async def commit_block(self, block: Block) -> None:
        self._node.blockchain.append(block)
        await self._node.db.persist_block(block)
        await self._node.gossip_manager.clear_pending_orders()
        self._node.gossip_manager.disable_full_broadcast()
        logger.info(
            "committed block %d trades=%d is_eod=%s",
            block.index, len(block.trades), block.is_eod,
        )
