from __future__ import annotations

import os
import logging
from dataclasses import dataclass, field
from datetime import time
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class BankConfig:
    bank_id: str
    host: str
    port: int
    db_url: str


@dataclass
class Config:
    this_bank: BankConfig
    peers: list[BankConfig]
    keys_dir: Path

    # Gossip
    gossip_fanout: int = 3

    # Auction timer (leader-controlled)
    auction_interval_seconds: int = 300  # 5 min default

    # Mínimo de trades no leilão para um bloco ser criado (blocos EOD são isentos)
    min_trades_per_block: int = 1

    # Block trigger (legacy fallback)
    rolling_window_size: int = 5
    volume_trigger_pct: float = 0.05
    block_interval_seconds: int = 1800

    # Timeouts
    sync_timeout_seconds: int = 10
    vote_timeout_seconds: int = 90          # leader waits this long for manager votes
    manager_vote_timeout_seconds: int = 75  # manager has this long to click before auto-fallback
    leader_timeout_seconds: int = 15
    heartbeat_interval_seconds: int = 5
    peer_timeout_seconds: int = 20

    # Market hours
    market_open: time = field(default_factory=lambda: time(10, 0))
    market_close: time = field(default_factory=lambda: time(18, 0))
    order_cutoff: time = field(default_factory=lambda: time(17, 55))

    # HTTP API
    api_port: int = 8000

    # Stocks
    stock_universe: list[str] = field(default_factory=lambda: [
        "PETR4", "VALE3", "ITUB4", "BBDC4", "ABEV3",
        "WEGE3", "RENT3", "BBAS3", "SUZB3", "RDOR3",
        "RADL3", "EGIE3", "LREN3", "HAPV3", "MGLU3",
    ])


def _env(key: str, default: str = "") -> str:
    return os.environ.get(key, default)


def _env_int(key: str, default: int) -> int:
    val = os.environ.get(key)
    return int(val) if val is not None else default


def _env_float(key: str, default: float) -> float:
    val = os.environ.get(key)
    return float(val) if val is not None else default


def load_config() -> Config:
    """Build Config from environment variables."""
    bank_id = _env("BANK_ID")
    if not bank_id:
        raise ValueError("BANK_ID environment variable is required")

    this_bank = BankConfig(
        bank_id=bank_id,
        host=_env("BANK_HOST", "0.0.0.0"),
        port=_env_int("BANK_PORT", 9000),
        db_url=_env("DB_URL", f"postgresql://exchange:exchange@localhost/exchange_{bank_id}"),
    )

    peers: list[BankConfig] = []
    for i in range(10):
        peer_env = _env(f"PEER_{i}")
        if not peer_env:
            break
        # format: bank_id:host:port:db_url  OR  bank_id:host:port (db_url omitted)
        parts = peer_env.split(":", 3)
        if len(parts) < 3:
            logger.warning("malformed PEER_%d: %s", i, peer_env)
            continue
        peer_bank_id, peer_host, peer_port_str = parts[0], parts[1], parts[2]
        peer_db_url = parts[3] if len(parts) == 4 else ""
        peers.append(BankConfig(
            bank_id=peer_bank_id,
            host=peer_host,
            port=int(peer_port_str),
            db_url=peer_db_url,
        ))

    keys_dir = Path(_env("KEYS_DIR", "keys"))

    return Config(
        this_bank=this_bank,
        peers=peers,
        keys_dir=keys_dir,
        gossip_fanout=_env_int("GOSSIP_FANOUT", 3),
        rolling_window_size=_env_int("ROLLING_WINDOW_SIZE", 5),
        volume_trigger_pct=_env_float("VOLUME_TRIGGER_PCT", 0.05),
        auction_interval_seconds=_env_int("AUCTION_INTERVAL_SECONDS", 300),
        min_trades_per_block=_env_int("MIN_TRADES_PER_BLOCK", 1),
        block_interval_seconds=_env_int("BLOCK_INTERVAL_SECONDS", 1800),
        sync_timeout_seconds=_env_int("SYNC_TIMEOUT_SECONDS", 10),
        vote_timeout_seconds=_env_int("VOTE_TIMEOUT_SECONDS", 90),
        manager_vote_timeout_seconds=_env_int("MANAGER_VOTE_TIMEOUT_SECONDS", 75),
        leader_timeout_seconds=_env_int("LEADER_TIMEOUT_SECONDS", 15),
        heartbeat_interval_seconds=_env_int("HEARTBEAT_INTERVAL_SECONDS", 5),
        peer_timeout_seconds=_env_int("PEER_TIMEOUT_SECONDS", 20),
        api_port=_env_int("API_PORT", 8000),
    )
