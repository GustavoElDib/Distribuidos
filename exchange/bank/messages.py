from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Any

import logging

logger = logging.getLogger(__name__)


class MessageType(str, Enum):
    ORDER_GOSSIP = "ORDER_GOSSIP"
    CLOSE_WINDOW = "CLOSE_WINDOW"
    SYNC_ORDERS = "SYNC_ORDERS"
    SYNC_ACK = "SYNC_ACK"
    BLOCK_CANDIDATE = "BLOCK_CANDIDATE"
    BLOCK_VOTE = "BLOCK_VOTE"
    BLOCK_COMMIT = "BLOCK_COMMIT"
    CHAIN_SYNC_REQUEST = "CHAIN_SYNC_REQUEST"
    CHAIN_SYNC_RESPONSE = "CHAIN_SYNC_RESPONSE"
    HEARTBEAT = "HEARTBEAT"


@dataclass
class Message:
    msg_type: MessageType
    sender_id: str
    payload: dict[str, Any]
    msg_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_json(self) -> str:
        return json.dumps({
            "msg_type": self.msg_type.value,
            "sender_id": self.sender_id,
            "msg_id": self.msg_id,
            "timestamp": self.timestamp,
            "payload": self.payload,
        })

    @classmethod
    def from_json(cls, raw: str) -> "Message":
        data = json.loads(raw)
        return cls(
            msg_type=MessageType(data["msg_type"]),
            sender_id=data["sender_id"],
            msg_id=data["msg_id"],
            timestamp=data["timestamp"],
            payload=data["payload"],
        )

    def encode(self) -> bytes:
        return (self.to_json() + "\n").encode("utf-8")


# ---------------------------------------------------------------------------
# Typed payload dataclasses (serialized as dicts inside Message.payload)
# ---------------------------------------------------------------------------

@dataclass
class OrderGossipPayload:
    order: dict[str, Any]          # Order serialized as dict

    def to_dict(self) -> dict:
        return {"order": self.order}

    @classmethod
    def from_dict(cls, d: dict) -> "OrderGossipPayload":
        return cls(order=d["order"])


@dataclass
class CloseWindowPayload:
    block_index: int

    def to_dict(self) -> dict:
        return {"block_index": self.block_index}

    @classmethod
    def from_dict(cls, d: dict) -> "CloseWindowPayload":
        return cls(block_index=d["block_index"])


@dataclass
class SyncOrdersPayload:
    orders: list[dict[str, Any]]   # list of Order dicts
    block_index: int

    def to_dict(self) -> dict:
        return {"orders": self.orders, "block_index": self.block_index}

    @classmethod
    def from_dict(cls, d: dict) -> "SyncOrdersPayload":
        return cls(orders=d["orders"], block_index=d["block_index"])


@dataclass
class SyncAckPayload:
    block_index: int
    from_bank_id: str

    def to_dict(self) -> dict:
        return {"block_index": self.block_index, "from_bank_id": self.from_bank_id}

    @classmethod
    def from_dict(cls, d: dict) -> "SyncAckPayload":
        return cls(block_index=d["block_index"], from_bank_id=d["from_bank_id"])


@dataclass
class BlockCandidatePayload:
    block: dict[str, Any]          # Block serialized as dict

    def to_dict(self) -> dict:
        return {"block": self.block}

    @classmethod
    def from_dict(cls, d: dict) -> "BlockCandidatePayload":
        return cls(block=d["block"])


@dataclass
class BlockVotePayload:
    block_index: int
    block_hash: str
    accepted: bool
    reason: str = ""

    def to_dict(self) -> dict:
        return {
            "block_index": self.block_index,
            "block_hash": self.block_hash,
            "accepted": self.accepted,
            "reason": self.reason,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "BlockVotePayload":
        return cls(
            block_index=d["block_index"],
            block_hash=d["block_hash"],
            accepted=d["accepted"],
            reason=d.get("reason", ""),
        )


@dataclass
class BlockCommitPayload:
    block: dict[str, Any]

    def to_dict(self) -> dict:
        return {"block": self.block}

    @classmethod
    def from_dict(cls, d: dict) -> "BlockCommitPayload":
        return cls(block=d["block"])


@dataclass
class ChainSyncRequestPayload:
    from_index: int

    def to_dict(self) -> dict:
        return {"from_index": self.from_index}

    @classmethod
    def from_dict(cls, d: dict) -> "ChainSyncRequestPayload":
        return cls(from_index=d["from_index"])


@dataclass
class ChainSyncResponsePayload:
    blocks: list[dict[str, Any]]

    def to_dict(self) -> dict:
        return {"blocks": self.blocks}

    @classmethod
    def from_dict(cls, d: dict) -> "ChainSyncResponsePayload":
        return cls(blocks=d["blocks"])


@dataclass
class HeartbeatPayload:
    chain_length: int

    def to_dict(self) -> dict:
        return {"chain_length": self.chain_length}

    @classmethod
    def from_dict(cls, d: dict) -> "HeartbeatPayload":
        return cls(chain_length=d["chain_length"])


def build_message(
    msg_type: MessageType,
    sender_id: str,
    payload_obj: Any,
) -> Message:
    return Message(
        msg_type=msg_type,
        sender_id=sender_id,
        payload=payload_obj.to_dict(),
    )
