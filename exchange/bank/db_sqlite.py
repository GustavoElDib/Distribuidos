"""SQLite backend — drop-in replacement for db.Database (local dev, no PostgreSQL needed)."""
from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime, timezone
from typing import Optional

import aiosqlite

from .blockchain import Block, EodSnapshot, Order, Trade

logger = logging.getLogger(__name__)

INITIAL_CASH_BALANCE = 100_000.00

_SCHEMA = """
CREATE TABLE IF NOT EXISTS blocks (
    idx             INTEGER PRIMARY KEY,
    timestamp       TEXT NOT NULL,
    previous_hash   TEXT NOT NULL,
    producer_id     TEXT NOT NULL,
    block_hash      TEXT NOT NULL UNIQUE,
    signature       TEXT NOT NULL,
    is_eod          INTEGER NOT NULL DEFAULT 0,
    raw_json        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
    order_id        TEXT PRIMARY KEY,
    investor_id     TEXT NOT NULL,
    bank_id         TEXT NOT NULL,
    stock           TEXT NOT NULL,
    side            TEXT NOT NULL,
    quantity        INTEGER NOT NULL,
    limit_price     REAL NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',
    filled_quantity INTEGER NOT NULL DEFAULT 0,
    submitted_at    TEXT NOT NULL,
    block_index     INTEGER
);

CREATE TABLE IF NOT EXISTS trades (
    trade_id        TEXT PRIMARY KEY,
    stock           TEXT NOT NULL,
    buyer_order_id  TEXT NOT NULL,
    seller_order_id TEXT NOT NULL,
    buyer_bank_id   TEXT NOT NULL,
    seller_bank_id  TEXT NOT NULL,
    quantity        INTEGER NOT NULL,
    price           REAL NOT NULL,
    block_index     INTEGER NOT NULL,
    traded_at       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS investors (
    investor_id     TEXT PRIMARY KEY,
    bank_id         TEXT NOT NULL,
    cash_balance    REAL NOT NULL DEFAULT 0,
    cash_reserved   REAL NOT NULL DEFAULT 0,
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS portfolios (
    investor_id     TEXT NOT NULL,
    stock           TEXT NOT NULL,
    quantity        INTEGER NOT NULL DEFAULT 0,
    reserved        INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (investor_id, stock)
);

CREATE TABLE IF NOT EXISTS price_history (
    stock           TEXT NOT NULL,
    block_index     INTEGER NOT NULL,
    clearing_price  REAL NOT NULL,
    volume          INTEGER NOT NULL,
    recorded_at     TEXT NOT NULL,
    PRIMARY KEY (stock, block_index)
);

CREATE TABLE IF NOT EXISTS daily_ohlc (
    stock           TEXT NOT NULL,
    trade_date      TEXT NOT NULL,
    open_price      REAL,
    high_price      REAL,
    low_price       REAL,
    close_price     REAL,
    total_volume    INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (stock, trade_date)
);
"""


class SqliteDatabase:
    def __init__(self, db_url: str) -> None:
        # accept sqlite:///./path or sqlite:////abs/path
        path = db_url.removeprefix("sqlite:///")
        self._db_path = os.path.abspath(path)

    def _connect(self):
        return aiosqlite.connect(self._db_path)

    async def initialize(self) -> None:
        os.makedirs(os.path.dirname(self._db_path), exist_ok=True)
        async with self._connect() as conn:
            conn.row_factory = aiosqlite.Row
            await conn.executescript(_SCHEMA)
            # Migrate databases created before the filled_quantity column existed.
            try:
                await conn.execute(
                    "ALTER TABLE orders ADD COLUMN filled_quantity INTEGER NOT NULL DEFAULT 0"
                )
            except aiosqlite.OperationalError:
                pass  # column already exists
            await conn.commit()
        logger.info("sqlite db initialised at %s", self._db_path)

    async def close(self) -> None:
        pass

    # ------------------------------------------------------------------
    # Blocks
    # ------------------------------------------------------------------

    async def insert_block(self, block: Block) -> None:
        async with self._connect() as conn:
            await conn.execute(
                "INSERT OR IGNORE INTO blocks "
                "(idx, timestamp, previous_hash, producer_id, "
                "block_hash, signature, is_eod, raw_json) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (block.index, block.timestamp, block.previous_hash,
                 block.producer_id, block.block_hash, block.signature,
                 int(block.is_eod), json.dumps(block.to_dict())),
            )
            await conn.commit()

    async def get_block_by_index(self, index: int) -> Optional[Block]:
        async with self._connect() as conn:
            conn.row_factory = aiosqlite.Row
            cur = await conn.execute("SELECT raw_json FROM blocks WHERE idx=?", (index,))
            row = await cur.fetchone()
        return Block.from_dict(json.loads(row["raw_json"])) if row else None

    async def get_blocks_from(self, index: int) -> list[Block]:
        async with self._connect() as conn:
            conn.row_factory = aiosqlite.Row
            cur = await conn.execute(
                "SELECT raw_json FROM blocks WHERE idx>=? ORDER BY idx", (index,)
            )
            rows = await cur.fetchall()
        return [Block.from_dict(json.loads(r["raw_json"])) for r in rows]

    # ------------------------------------------------------------------
    # Orders
    # ------------------------------------------------------------------

    async def insert_orders(self, orders: list[Order]) -> None:
        if not orders:
            return
        async with self._connect() as conn:
            await conn.executemany(
                "INSERT OR IGNORE INTO orders "
                "(order_id, investor_id, bank_id, stock, side, quantity, "
                "limit_price, status, submitted_at) VALUES (?,?,?,?,?,?,?,'pending',?)",
                [(o.order_id, o.investor_id, o.bank_id, o.stock, o.side,
                  o.quantity, o.limit_price, o.timestamp) for o in orders],
            )
            await conn.commit()

    async def update_order_status(
        self, order_id: str, status: str, block_index: int, filled_quantity: int = 0
    ) -> None:
        async with self._connect() as conn:
            await conn.execute(
                "UPDATE orders SET status=?, block_index=?, filled_quantity=? WHERE order_id=?",
                (status, block_index, filled_quantity, order_id),
            )
            await conn.commit()

    # ------------------------------------------------------------------
    # Trades
    # ------------------------------------------------------------------

    async def insert_trades(self, trades: list[Trade]) -> None:
        if not trades:
            return
        now = datetime.now(timezone.utc).isoformat()
        async with self._connect() as conn:
            await conn.executemany(
                "INSERT OR IGNORE INTO trades "
                "(trade_id, stock, buyer_order_id, seller_order_id, "
                "buyer_bank_id, seller_bank_id, quantity, price, block_index, traded_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                [(t.trade_id, t.stock, t.buyer_order_id, t.seller_order_id,
                  t.buyer_bank_id, t.seller_bank_id, t.quantity, t.price,
                  t.block_index, now) for t in trades],
            )
            await conn.commit()

    # ------------------------------------------------------------------
    # Price history
    # ------------------------------------------------------------------

    async def upsert_price_history(
        self, stock: str, block_index: int, price: float, volume: int
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        async with self._connect() as conn:
            await conn.execute(
                "INSERT INTO price_history (stock, block_index, clearing_price, volume, recorded_at) "
                "VALUES (?,?,?,?,?) "
                "ON CONFLICT(stock, block_index) DO UPDATE SET "
                "clearing_price=excluded.clearing_price, volume=excluded.volume",
                (stock, block_index, price, volume, now),
            )
            await conn.commit()

    async def get_price_history(self, stock: str, limit: int = 100) -> list[dict]:
        async with self._connect() as conn:
            conn.row_factory = aiosqlite.Row
            cur = await conn.execute(
                "SELECT stock, block_index, clearing_price, volume, recorded_at "
                "FROM price_history WHERE stock=? ORDER BY block_index DESC LIMIT ?",
                (stock, limit),
            )
            rows = await cur.fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Investors and portfolios
    # ------------------------------------------------------------------

    async def get_investor(self, investor_id: str) -> Optional[dict]:
        async with self._connect() as conn:
            conn.row_factory = aiosqlite.Row
            cur = await conn.execute(
                "SELECT investor_id, bank_id, cash_balance, cash_reserved, created_at "
                "FROM investors WHERE investor_id=?",
                (investor_id,),
            )
            row = await cur.fetchone()
        return dict(row) if row else None

    async def ensure_investor(self, investor_id: str, bank_id: str) -> None:
        async with self._connect() as conn:
            await conn.execute(
                "INSERT INTO investors (investor_id, bank_id, cash_balance, cash_reserved) "
                "VALUES (?,?,?,0) "
                "ON CONFLICT(investor_id) DO NOTHING",
                (investor_id, bank_id, INITIAL_CASH_BALANCE),
            )
            await conn.commit()

    async def update_investor_balances(
        self, investor_id: str, cash_delta: float, cash_reserved_delta: float
    ) -> None:
        async with self._connect() as conn:
            await conn.execute(
                "UPDATE investors SET "
                "cash_balance=cash_balance+?, cash_reserved=cash_reserved+? "
                "WHERE investor_id=?",
                (cash_delta, cash_reserved_delta, investor_id),
            )
            await conn.commit()

    async def upsert_portfolio(
        self, investor_id: str, stock: str, qty_delta: int, reserved_delta: int
    ) -> None:
        async with self._connect() as conn:
            await conn.execute(
                "INSERT INTO portfolios (investor_id, stock, quantity, reserved) "
                "VALUES (?,?,?,?) "
                "ON CONFLICT(investor_id, stock) DO UPDATE SET "
                "quantity=portfolios.quantity+excluded.quantity, "
                "reserved=portfolios.reserved+excluded.reserved",
                (investor_id, stock, qty_delta, reserved_delta),
            )
            await conn.commit()

    # ------------------------------------------------------------------
    # EOD OHLC
    # ------------------------------------------------------------------

    async def insert_eod_ohlc(self, ohlc: dict, trade_date: date) -> None:
        td = trade_date.isoformat() if isinstance(trade_date, date) else str(trade_date)
        async with self._connect() as conn:
            await conn.executemany(
                "INSERT INTO daily_ohlc "
                "(stock, trade_date, open_price, high_price, low_price, close_price, total_volume) "
                "VALUES (?,?,?,?,?,?,?) "
                "ON CONFLICT(stock, trade_date) DO UPDATE SET "
                "open_price=excluded.open_price, high_price=excluded.high_price, "
                "low_price=excluded.low_price, close_price=excluded.close_price, "
                "total_volume=excluded.total_volume",
                [(stock, td, d.get("open"), d.get("high"), d.get("low"),
                  d.get("close"), d.get("volume", 0))
                 for stock, d in ohlc.items()],
            )
            await conn.commit()

    # ------------------------------------------------------------------
    # EOD snapshot
    # ------------------------------------------------------------------

    async def build_eod_snapshot(self) -> EodSnapshot:
        async with self._connect() as conn:
            conn.row_factory = aiosqlite.Row

            inv_cur = await conn.execute("SELECT investor_id, cash_balance FROM investors")
            inv_rows = await inv_cur.fetchall()

            port_cur = await conn.execute(
                "SELECT investor_id, stock, quantity FROM portfolios WHERE quantity > 0"
            )
            port_rows = await port_cur.fetchall()

            agg_cur = await conn.execute("""
                SELECT stock,
                       MIN(block_index) AS first_idx,
                       MAX(block_index) AS last_idx,
                       MAX(clearing_price) AS high_price,
                       MIN(clearing_price) AS low_price,
                       SUM(volume) AS total_volume
                FROM price_history
                WHERE block_index >= (
                    SELECT COALESCE(MAX(idx), 0) FROM blocks WHERE is_eod=1
                )
                GROUP BY stock
            """)
            agg_rows = await agg_cur.fetchall()

            daily_ohlc: dict[str, dict] = {}
            for row in agg_rows:
                stock = row["stock"]
                open_cur = await conn.execute(
                    "SELECT clearing_price FROM price_history WHERE stock=? AND block_index=?",
                    (stock, row["first_idx"]),
                )
                open_row = await open_cur.fetchone()
                close_cur = await conn.execute(
                    "SELECT clearing_price FROM price_history WHERE stock=? AND block_index=?",
                    (stock, row["last_idx"]),
                )
                close_row = await close_cur.fetchone()
                daily_ohlc[stock] = {
                    "open": float(open_row[0]) if open_row else 0.0,
                    "high": float(row["high_price"]),
                    "low": float(row["low_price"]),
                    "close": float(close_row[0]) if close_row else 0.0,
                    "volume": int(row["total_volume"]),
                }

        portfolios: dict[str, dict] = {}
        for row in inv_rows:
            portfolios[row["investor_id"]] = {
                "cash": float(row["cash_balance"]), "shares": {}
            }
        for row in port_rows:
            inv_id = row["investor_id"]
            if inv_id not in portfolios:
                portfolios[inv_id] = {"cash": 0.0, "shares": {}}
            portfolios[inv_id]["shares"][row["stock"]] = row["quantity"]

        return EodSnapshot(portfolios=portfolios, daily_ohlc=daily_ohlc)

    # ------------------------------------------------------------------
    # Post-block settlement
    # ------------------------------------------------------------------

    async def persist_block(self, block: Block) -> None:
        await self.insert_block(block)
        await self.insert_orders(block.orders)
        order_by_id = {o.order_id: o for o in block.orders}
        for order in block.orders:
            await self.ensure_investor(order.investor_id, order.bank_id)
        # A call-auction round is single-shot: any quantity an order doesn't
        # fill here is cancelled, not carried over to a future round. Track
        # the actual filled quantity so partial fills aren't reported as if
        # the whole order matched.
        filled_qty: dict[str, int] = {}
        for trade in block.trades:
            filled_qty[trade.buyer_order_id] = filled_qty.get(trade.buyer_order_id, 0) + trade.quantity
            filled_qty[trade.seller_order_id] = filled_qty.get(trade.seller_order_id, 0) + trade.quantity

        for order in block.orders:
            filled = filled_qty.get(order.order_id, 0)
            if filled >= order.quantity:
                status = "matched"
            elif filled > 0:
                status = "partial"
            else:
                status = "cancelled" if block.is_eod else "expired"
            await self.update_order_status(order.order_id, status, block.index, filled)
        await self.insert_trades(block.trades)
        for trade in block.trades:
            notional = trade.quantity * trade.price
            buyer = order_by_id.get(trade.buyer_order_id)
            seller = order_by_id.get(trade.seller_order_id)
            if buyer is not None:
                await self.update_investor_balances(buyer.investor_id, -notional, 0)
                await self.upsert_portfolio(buyer.investor_id, trade.stock, trade.quantity, 0)
            if seller is not None:
                await self.update_investor_balances(seller.investor_id, notional, 0)
                await self.upsert_portfolio(seller.investor_id, trade.stock, -trade.quantity, 0)
        for stock, price in block.clearing_prices.items():
            volume = sum(t.quantity for t in block.trades if t.stock == stock)
            await self.upsert_price_history(stock, block.index, price, volume)
        if block.is_eod and block.eod_snapshot:
            await self.insert_eod_ohlc(
                block.eod_snapshot.daily_ohlc, datetime.now(timezone.utc).date()
            )

    # ------------------------------------------------------------------
    # API query helpers
    # ------------------------------------------------------------------

    async def get_recent_orders(self, limit: int = 50) -> list[dict]:
        async with self._connect() as conn:
            conn.row_factory = aiosqlite.Row
            cur = await conn.execute(
                "SELECT order_id, investor_id, bank_id, stock, side, quantity, "
                "limit_price, status, filled_quantity, submitted_at, block_index "
                "FROM orders ORDER BY submitted_at DESC LIMIT ?",
                (limit,),
            )
            rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def get_recent_trades(self, limit: int = 50) -> list[dict]:
        async with self._connect() as conn:
            conn.row_factory = aiosqlite.Row
            cur = await conn.execute(
                "SELECT trade_id, stock, buyer_bank_id, seller_bank_id, "
                "quantity, price, block_index, traded_at "
                "FROM trades ORDER BY traded_at DESC LIMIT ?",
                (limit,),
            )
            rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def get_investor_portfolio(self, investor_id: str) -> Optional[tuple]:
        inv = await self.get_investor(investor_id)
        if inv is None:
            return None
        async with self._connect() as conn:
            conn.row_factory = aiosqlite.Row
            cur = await conn.execute(
                "SELECT stock, quantity, reserved FROM portfolios "
                "WHERE investor_id=? AND quantity>0",
                (investor_id,),
            )
            rows = await cur.fetchall()
        return inv, [dict(r) for r in rows]
