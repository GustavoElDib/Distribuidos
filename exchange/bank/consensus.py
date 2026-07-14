from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from .auction import run_call_auction
from .blockchain import Block, EodSnapshot, Trade
from .crypto import verify_block
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
        self._byzantine_nodes: set[str] = set()

        # Consensus round for the current block height. Incremented (rotating
        # the leader) whenever a proposed block is rejected by manager votes;
        # reset to 0 once a block is committed.
        self._round: int = 0

        # Live vote tracking for dashboard visibility
        self._vote_state: dict[str, str] = {}   # peer_id -> "pending"/"accepted"/"rejected"/"timeout"/"byzantine"
        self._voting_block_index: int | None = None
        self._voting_since: float | None = None  # asyncio loop time when round started
        self._last_vote_result: dict | None = None  # final result of last round

    def initialize_bank_ids(self, all_bank_ids: list[str]) -> None:
        self._sorted_bank_ids = sorted(all_bank_ids)

    @property
    def byzantine_nodes(self) -> set[str]:
        return self._byzantine_nodes

    def get_vote_status(self) -> dict:
        """Return current or last voting round status for API/dashboard."""
        now = asyncio.get_event_loop().time()
        elapsed = round(now - self._voting_since, 1) if self._voting_since else None
        return {
            "active": self._voting_block_index is not None,
            "block_index": self._voting_block_index,
            "elapsed_seconds": elapsed,
            "votes": dict(self._vote_state),
            "last_result": self._last_vote_result,
        }

    @property
    def bft_f(self) -> int:
        """Maximum number of Byzantine faults tolerable (floor((n-1)/3))."""
        n = len(self._sorted_bank_ids)
        return (n - 1) // 3

    @property
    def bft_quorum(self) -> int:
        """Minimum accept votes required for BFT consensus (2f+1)."""
        return 2 * self.bft_f + 1

    def get_current_leader(self, block_index: int) -> str:
        n = len(self._sorted_bank_ids)
        return self._sorted_bank_ids[(block_index + self._round) % n]

    @property
    def current_round(self) -> int:
        return self._round

    def advance_round(self) -> int:
        """Rotate leadership to the next bank after a rejected block."""
        self._round += 1
        return self._round

    def set_round(self, value: int) -> None:
        """Adopt the round announced by the leader (keeps nodes in sync)."""
        self._round = value

    def reset_round(self) -> None:
        self._round = 0

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

        # Initialize live vote state (leader counts as implicit accept)
        self._voting_block_index = candidate.index
        self._voting_since = loop.time()
        self._vote_state = {pid: "pending" for pid in peer_ids}
        self._vote_state[cfg.this_bank.bank_id] = "accepted"  # leader's own vote

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

        # Leader counts its own implicit accept vote
        accept = 1
        reject = 0
        responding = 1

        for peer_id, result in zip(peer_ids, results):
            if peer_id in self._byzantine_nodes:
                logger.warning("vote: ignoring known Byzantine node %s", peer_id)
                self._vote_state[peer_id] = "byzantine"
                continue
            if isinstance(result, Exception):
                logger.warning("vote: peer %s timed out", peer_id)
                self._vote_state[peer_id] = "timeout"
                continue
            responding += 1
            if result.accepted:
                accept += 1
                self._vote_state[peer_id] = "accepted"
            else:
                reject += 1
                self._vote_state[peer_id] = "rejected"

        self._vote_futures.clear()

        # BFT quorum: require 2f+1 accepts (f = floor((n-1)/3))
        quorum = self.bft_quorum
        accepted = accept >= quorum

        logger.info(
            "voting round block %d: accept=%d reject=%d quorum=%d bft_f=%d -> %s",
            candidate.index, accept, reject, quorum, self.bft_f,
            "ACCEPTED" if accepted else "REJECTED",
        )

        self._last_vote_result = {
            "block_index": candidate.index,
            "accepted": accepted,
            "accept_count": accept,
            "reject_count": reject,
            "quorum": quorum,
            "votes": dict(self._vote_state),
        }
        # Clear active round (round is done)
        self._voting_block_index = None
        self._voting_since = None

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
        # Validate vote signature for Byzantine detection
        if payload.signature:
            peer_key = self._node.peer_keys.get(sender_id)
            if peer_key is None:
                logger.warning("byzantine: vote from unknown peer %s", sender_id)
                self._byzantine_nodes.add(sender_id)
                return
            vote_data = f"{payload.block_index}:{payload.block_hash}:{payload.accepted}"
            if not verify_block(peer_key, vote_data, payload.signature):
                logger.warning(
                    "byzantine: invalid vote signature from %s on block %d",
                    sender_id, payload.block_index,
                )
                self._byzantine_nodes.add(sender_id)
                return
        else:
            logger.warning("vote from %s missing signature (block %d)", sender_id, payload.block_index)

        fut = self._vote_futures.get(sender_id)
        if fut is None:
            return
        if fut.done():
            # Detect conflicting votes from same node (equivocation)
            try:
                prev = fut.result()
                if prev.accepted != payload.accepted:
                    logger.warning(
                        "byzantine: equivocating vote from %s on block %d",
                        sender_id, payload.block_index,
                    )
                    self._byzantine_nodes.add(sender_id)
                    self._vote_state[sender_id] = "byzantine"
            except Exception:
                pass
            return
        # Update live vote state immediately so dashboard can show it
        self._vote_state[sender_id] = "accepted" if payload.accepted else "rejected"
        fut.set_result(payload)

    async def commit_block(self, block: Block) -> None:
        self._node.blockchain.append(block)
        await self._node.db.persist_block(block)
        await self._node.gossip_manager.clear_pending_orders()
        # New block height starts a fresh round → normal leader rotation resumes.
        self.reset_round()
        logger.info(
            "committed block %d trades=%d is_eod=%s",
            block.index, len(block.trades), block.is_eod,
        )
