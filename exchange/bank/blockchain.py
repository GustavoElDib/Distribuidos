from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class Order:
    order_id: str
    investor_id: str
    bank_id: str
    stock: str
    side: str           # "buy" or "sell"
    quantity: int
    limit_price: float
    timestamp: str      # ISO8601 UTC

    def to_dict(self) -> dict:
        return {
            "order_id": self.order_id,
            "investor_id": self.investor_id,
            "bank_id": self.bank_id,
            "stock": self.stock,
            "side": self.side,
            "quantity": self.quantity,
            "limit_price": self.limit_price,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Order":
        return cls(
            order_id=d["order_id"],
            investor_id=d["investor_id"],
            bank_id=d["bank_id"],
            stock=d["stock"],
            side=d["side"],
            quantity=int(d["quantity"]),
            limit_price=float(d["limit_price"]),
            timestamp=d["timestamp"],
        )


@dataclass
class Trade:
    trade_id: str
    stock: str
    buyer_order_id: str
    seller_order_id: str
    buyer_bank_id: str
    seller_bank_id: str
    quantity: int
    price: float
    block_index: int

    def to_dict(self) -> dict:
        return {
            "trade_id": self.trade_id,
            "stock": self.stock,
            "buyer_order_id": self.buyer_order_id,
            "seller_order_id": self.seller_order_id,
            "buyer_bank_id": self.buyer_bank_id,
            "seller_bank_id": self.seller_bank_id,
            "quantity": self.quantity,
            "price": self.price,
            "block_index": self.block_index,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Trade":
        return cls(
            trade_id=d["trade_id"],
            stock=d["stock"],
            buyer_order_id=d["buyer_order_id"],
            seller_order_id=d["seller_order_id"],
            buyer_bank_id=d["buyer_bank_id"],
            seller_bank_id=d["seller_bank_id"],
            quantity=int(d["quantity"]),
            price=float(d["price"]),
            block_index=int(d["block_index"]),
        )


@dataclass
class EodSnapshot:
    portfolios: dict[str, dict]     # investor_id -> {"cash": float, "shares": {stock: int}}
    daily_ohlc: dict[str, dict]     # stock -> {"open", "high", "low", "close", "volume"}

    def to_dict(self) -> dict:
        return {"portfolios": self.portfolios, "daily_ohlc": self.daily_ohlc}

    @classmethod
    def from_dict(cls, d: dict) -> "EodSnapshot":
        return cls(portfolios=d["portfolios"], daily_ohlc=d["daily_ohlc"])


@dataclass
class Block:
    index: int
    timestamp: str
    previous_hash: str
    producer_id: str
    orders: list[Order]
    trades: list[Trade]
    clearing_prices: dict[str, float]
    merkle_root: str
    is_eod: bool
    eod_snapshot: Optional[EodSnapshot]
    block_hash: str
    signature: str

    def to_dict(self) -> dict:
        return {
            "index": self.index,
            "timestamp": self.timestamp,
            "previous_hash": self.previous_hash,
            "producer_id": self.producer_id,
            "orders": [o.to_dict() for o in self.orders],
            "trades": [t.to_dict() for t in self.trades],
            "clearing_prices": self.clearing_prices,
            "merkle_root": self.merkle_root,
            "is_eod": self.is_eod,
            "eod_snapshot": self.eod_snapshot.to_dict() if self.eod_snapshot else None,
            "block_hash": self.block_hash,
            "signature": self.signature,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Block":
        return cls(
            index=int(d["index"]),
            timestamp=d["timestamp"],
            previous_hash=d["previous_hash"],
            producer_id=d["producer_id"],
            orders=[Order.from_dict(o) for o in d.get("orders", [])],
            trades=[Trade.from_dict(t) for t in d.get("trades", [])],
            clearing_prices={k: float(v) for k, v in d.get("clearing_prices", {}).items()},
            merkle_root=d["merkle_root"],
            is_eod=bool(d["is_eod"]),
            eod_snapshot=EodSnapshot.from_dict(d["eod_snapshot"]) if d.get("eod_snapshot") else None,
            block_hash=d["block_hash"],
            signature=d["signature"],
        )


def _canonical_json(obj: dict) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))


class BlockChain:
    def __init__(self) -> None:
        self._chain: list[Block] = []
        # order_ids de todos os blocos commitados — impede que uma ordem já
        # incluída em bloco (executada ou não) entre novamente em um bloco futuro
        self._committed_order_ids: set[str] = set()
        self._init_genesis()

    def _init_genesis(self) -> None:
        genesis = Block(
            index=0,
            timestamp="1970-01-01T00:00:00+00:00",  # fixed so all nodes produce identical hash
            previous_hash="0" * 64,
            producer_id="genesis",
            orders=[],
            trades=[],
            clearing_prices={},
            merkle_root=self.compute_merkle_root([]),
            is_eod=False,
            eod_snapshot=None,
            block_hash="",
            signature="",
        )
        genesis.block_hash = self.compute_block_hash(genesis)
        self._chain.append(genesis)

    def append(self, block: Block) -> None:
        last = self._chain[-1]
        if block.previous_hash != last.block_hash:
            raise ValueError(
                f"block {block.index}: previous_hash mismatch "
                f"(expected {last.block_hash}, got {block.previous_hash})"
            )
        if block.index != last.index + 1:
            raise ValueError(
                f"block index gap: expected {last.index + 1}, got {block.index}"
            )
        expected_hash = self.compute_block_hash(block)
        if block.block_hash != expected_hash:
            raise ValueError(
                f"block {block.index}: block_hash is invalid"
            )
        duplicated = [
            o.order_id for o in block.orders
            if o.order_id in self._committed_order_ids
        ]
        if duplicated:
            raise ValueError(
                f"block {block.index}: {len(duplicated)} order(s) already "
                f"committed in earlier blocks — duplicate trade rejected"
            )
        self._chain.append(block)
        self._committed_order_ids.update(o.order_id for o in block.orders)
        logger.info("appended block %d (producer=%s)", block.index, block.producer_id)

    @property
    def committed_order_ids(self) -> set[str]:
        return self._committed_order_ids

    def get_last_block(self) -> Block:
        return self._chain[-1]

    def get_block(self, index: int) -> Block:
        if index < 0 or index >= len(self._chain):
            raise IndexError(f"block index {index} out of range")
        return self._chain[index]

    def get_blocks_from(self, index: int) -> list[Block]:
        return self._chain[index:]

    def __len__(self) -> int:
        return len(self._chain)

    def compute_block_hash(self, block: Block) -> str:
        # hash all fields except block_hash and signature
        payload = {
            "index": block.index,
            "timestamp": block.timestamp,
            "previous_hash": block.previous_hash,
            "producer_id": block.producer_id,
            "orders": [o.to_dict() for o in block.orders],
            "trades": [t.to_dict() for t in block.trades],
            "clearing_prices": block.clearing_prices,
            "merkle_root": block.merkle_root,
            "is_eod": block.is_eod,
            "eod_snapshot": block.eod_snapshot.to_dict() if block.eod_snapshot else None,
        }
        return hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()

    def compute_merkle_root(self, trades: list[Trade]) -> str:
        if not trades:
            return hashlib.sha256(b"").hexdigest()
        leaves = [
            hashlib.sha256(_canonical_json(t.to_dict()).encode("utf-8")).hexdigest()
            for t in trades
        ]
        while len(leaves) > 1:
            if len(leaves) % 2 == 1:
                leaves.append(leaves[-1])
            leaves = [
                hashlib.sha256((leaves[i] + leaves[i + 1]).encode("utf-8")).hexdigest()
                for i in range(0, len(leaves), 2)
            ]
        return leaves[0]

    def validate_chain(self) -> bool:
        for i in range(1, len(self._chain)):
            block = self._chain[i]
            prev = self._chain[i - 1]
            if block.previous_hash != prev.block_hash:
                logger.error("chain broken at index %d: previous_hash mismatch", i)
                return False
            if block.block_hash != self.compute_block_hash(block):
                logger.error("chain broken at index %d: block_hash invalid", i)
                return False
            if block.merkle_root != self.compute_merkle_root(block.trades):
                logger.error("chain broken at index %d: merkle_root mismatch", i)
                return False
        return True
