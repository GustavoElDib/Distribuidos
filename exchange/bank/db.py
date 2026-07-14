from __future__ import annotations

import asyncio
import json
import logging
from datetime import date, datetime, timezone
from typing import Optional
from urllib.parse import urlsplit, urlunsplit

import asyncpg

from .blockchain import Block, Order, Trade, EodSnapshot

logger = logging.getLogger(__name__)

INITIAL_CASH_BALANCE = 100_000.00

_SCHEMA = """
CREATE TABLE IF NOT EXISTS blocks (
    index           INTEGER PRIMARY KEY,
    timestamp       TIMESTAMPTZ NOT NULL,
    previous_hash   TEXT NOT NULL,
    producer_id     TEXT NOT NULL,
    block_hash      TEXT NOT NULL UNIQUE,
    signature       TEXT NOT NULL,
    is_eod          BOOLEAN NOT NULL DEFAULT FALSE,
    raw_json        JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
    order_id        TEXT PRIMARY KEY,
    investor_id     TEXT NOT NULL,
    bank_id         TEXT NOT NULL,
    stock           TEXT NOT NULL,
    side            TEXT NOT NULL CHECK (side IN ('buy', 'sell')),
    quantity        INTEGER NOT NULL CHECK (quantity > 0),
    limit_price     NUMERIC(12, 2) NOT NULL CHECK (limit_price > 0),
    status          TEXT NOT NULL CHECK (status IN ('pending', 'matched', 'partial', 'expired', 'cancelled')),
    filled_quantity INTEGER NOT NULL DEFAULT 0 CHECK (filled_quantity >= 0),
    submitted_at    TIMESTAMPTZ NOT NULL,
    block_index     INTEGER REFERENCES blocks(index)
);

CREATE TABLE IF NOT EXISTS trades (
    trade_id            TEXT PRIMARY KEY,
    stock               TEXT NOT NULL,
    buyer_order_id      TEXT NOT NULL REFERENCES orders(order_id),
    seller_order_id     TEXT NOT NULL REFERENCES orders(order_id),
    buyer_bank_id       TEXT NOT NULL,
    seller_bank_id      TEXT NOT NULL,
    quantity            INTEGER NOT NULL,
    price               NUMERIC(12, 2) NOT NULL,
    block_index         INTEGER NOT NULL REFERENCES blocks(index),
    traded_at           TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS investors (
    investor_id     TEXT PRIMARY KEY,
    bank_id         TEXT NOT NULL,
    cash_balance    NUMERIC(14, 2) NOT NULL DEFAULT 0,
    cash_reserved   NUMERIC(14, 2) NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
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
    block_index     INTEGER NOT NULL REFERENCES blocks(index),
    clearing_price  NUMERIC(12, 2) NOT NULL,
    volume          INTEGER NOT NULL,
    recorded_at     TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (stock, block_index)
);

CREATE TABLE IF NOT EXISTS daily_ohlc (
    stock           TEXT NOT NULL,
    trade_date      DATE NOT NULL,
    open_price      NUMERIC(12, 2),
    high_price      NUMERIC(12, 2),
    low_price       NUMERIC(12, 2),
    close_price     NUMERIC(12, 2),
    total_volume    INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (stock, trade_date)
);
"""


class Database:
    def __init__(self, db_url: str) -> None:
        self._db_url = db_url
        self._pool: Optional[asyncpg.Pool] = None

    async def initialize(self) -> None:
        self._pool = await self._create_pool_with_retry()
        async with self._pool.acquire() as conn:
            await conn.execute(_SCHEMA)
            # Migrate tables created before the 'partial' fill status existed.
            await conn.execute(
                "ALTER TABLE orders ADD COLUMN IF NOT EXISTS filled_quantity "
                "INTEGER NOT NULL DEFAULT 0"
            )
            await conn.execute("ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_status_check")
            await conn.execute(
                "ALTER TABLE orders ADD CONSTRAINT orders_status_check "
                "CHECK (status IN ('pending', 'matched', 'partial', 'expired', 'cancelled'))"
            )
        logger.info("database initialized")

    async def _create_pool_with_retry(self) -> asyncpg.Pool:
        """Connect to Postgres, surviving two production failure modes:

        - the Postgres container is still starting when the bank comes up
          (connection refused / "the database system is starting up");
        - the target database does not exist because the Postgres data volume
          was initialised by an older compose file, before POSTGRES_DB was set
          (initdb only runs on an EMPTY volume, so env changes are ignored) —
          in that case we create the database ourselves and retry.
        """
        delay = 1.0
        last_exc: Optional[Exception] = None
        for _ in range(12):
            try:
                return await asyncpg.create_pool(
                    self._db_url, min_size=2, max_size=10
                )
            except asyncpg.InvalidCatalogNameError:
                await self._create_missing_database()
            except (OSError, TimeoutError, asyncpg.CannotConnectNowError) as exc:
                last_exc = exc
                logger.warning(
                    "postgres not ready (%s); retrying in %.0fs", exc, delay
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, 10.0)
        raise RuntimeError(
            f"could not connect to database at {self._db_url}"
        ) from last_exc

    async def _create_missing_database(self) -> None:
        """Create the target database via the maintenance db 'postgres'."""
        parts = urlsplit(self._db_url)
        dbname = parts.path.lstrip("/")
        admin_url = urlunsplit(parts._replace(path="/postgres"))
        logger.warning('database "%s" does not exist — creating it', dbname)
        conn = await asyncpg.connect(admin_url)
        try:
            safe_name = dbname.replace('"', '""')
            await conn.execute(f'CREATE DATABASE "{safe_name}"')
        except asyncpg.DuplicateDatabaseError:
            pass  # another replica created it between our check and now
        finally:
            await conn.close()

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()

    @property
    def pool(self) -> asyncpg.Pool:
        assert self._pool is not None, "Database.initialize() not called"
        return self._pool

    # ------------------------------------------------------------------
    # Blocks
    # ------------------------------------------------------------------

    async def insert_block(self, block: Block) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO blocks (index, timestamp, previous_hash, producer_id,
                                    block_hash, signature, is_eod, raw_json)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (index) DO NOTHING
                """,
                block.index,
                datetime.fromisoformat(block.timestamp),
                block.previous_hash,
                block.producer_id,
                block.block_hash,
                block.signature,
                block.is_eod,
                json.dumps(block.to_dict()),
            )

    async def get_block_by_index(self, index: int) -> Optional[Block]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT raw_json FROM blocks WHERE index = $1", index
            )
        if row is None:
            return None
        return Block.from_dict(json.loads(row["raw_json"]))

    async def get_blocks_from(self, index: int) -> list[Block]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT raw_json FROM blocks WHERE index >= $1 ORDER BY index", index
            )
        return [Block.from_dict(json.loads(r["raw_json"])) for r in rows]

    # ------------------------------------------------------------------
    # Orders
    # ------------------------------------------------------------------

    async def insert_orders(self, orders: list[Order]) -> None:
        if not orders:
            return
        async with self.pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO orders
                    (order_id, investor_id, bank_id, stock, side, quantity,
                     limit_price, status, submitted_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending', $8)
                ON CONFLICT (order_id) DO NOTHING
                """,
                [
                    (
                        o.order_id,
                        o.investor_id,
                        o.bank_id,
                        o.stock,
                        o.side,
                        o.quantity,
                        o.limit_price,
                        datetime.fromisoformat(o.timestamp),
                    )
                    for o in orders
                ],
            )

    async def update_order_status(
        self, order_id: str, status: str, block_index: int, filled_quantity: int = 0
    ) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE orders SET status=$1, block_index=$2, filled_quantity=$3 WHERE order_id=$4",
                status,
                block_index,
                filled_quantity,
                order_id,
            )

    # ------------------------------------------------------------------
    # Trades
    # ------------------------------------------------------------------

    async def insert_trades(self, trades: list[Trade]) -> None:
        if not trades:
            return
        now = datetime.now(timezone.utc)
        async with self.pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO trades
                    (trade_id, stock, buyer_order_id, seller_order_id,
                     buyer_bank_id, seller_bank_id, quantity, price, block_index, traded_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ON CONFLICT (trade_id) DO NOTHING
                """,
                [
                    (
                        t.trade_id, t.stock, t.buyer_order_id, t.seller_order_id,
                        t.buyer_bank_id, t.seller_bank_id, t.quantity, t.price,
                        t.block_index, now,
                    )
                    for t in trades
                ],
            )

    # ------------------------------------------------------------------
    # Price history
    # ------------------------------------------------------------------

    async def upsert_price_history(
        self, stock: str, block_index: int, price: float, volume: int
    ) -> None:
        now = datetime.now(timezone.utc)
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO price_history (stock, block_index, clearing_price, volume, recorded_at)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (stock, block_index) DO UPDATE
                    SET clearing_price=EXCLUDED.clearing_price, volume=EXCLUDED.volume
                """,
                stock, block_index, price, volume, now,
            )

    async def get_price_history(self, stock: str, limit: int = 100) -> list[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT stock, block_index, clearing_price, volume, recorded_at
                FROM price_history
                WHERE stock=$1
                ORDER BY block_index DESC
                LIMIT $2
                """,
                stock, limit,
            )
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Investors and portfolios
    # ------------------------------------------------------------------

    async def get_investor(self, investor_id: str) -> Optional[dict]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM investors WHERE investor_id=$1", investor_id
            )
        return dict(row) if row else None

    async def ensure_investor(self, investor_id: str, bank_id: str) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO investors (investor_id, bank_id, cash_balance, cash_reserved)
                VALUES ($1, $2, $3, 0)
                ON CONFLICT (investor_id) DO NOTHING
                """,
                investor_id, bank_id, INITIAL_CASH_BALANCE,
            )

    async def update_investor_balances(
        self,
        investor_id: str,
        cash_delta: float,
        cash_reserved_delta: float,
    ) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE investors
                SET cash_balance = cash_balance + $1,
                    cash_reserved = cash_reserved + $2
                WHERE investor_id=$3
                """,
                cash_delta, cash_reserved_delta, investor_id,
            )

    async def upsert_portfolio(
        self,
        investor_id: str,
        stock: str,
        qty_delta: int,
        reserved_delta: int,
    ) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO portfolios (investor_id, stock, quantity, reserved)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (investor_id, stock) DO UPDATE
                    SET quantity = portfolios.quantity + EXCLUDED.quantity,
                        reserved = portfolios.reserved + EXCLUDED.reserved
                """,
                investor_id, stock, qty_delta, reserved_delta,
            )

    # ------------------------------------------------------------------
    # EOD OHLC
    # ------------------------------------------------------------------

    async def insert_eod_ohlc(self, ohlc: dict[str, dict], trade_date: date) -> None:
        async with self.pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO daily_ohlc
                    (stock, trade_date, open_price, high_price, low_price, close_price, total_volume)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (stock, trade_date) DO UPDATE
                    SET open_price=EXCLUDED.open_price, high_price=EXCLUDED.high_price,
                        low_price=EXCLUDED.low_price, close_price=EXCLUDED.close_price,
                        total_volume=EXCLUDED.total_volume
                """,
                [
                    (
                        stock,
                        trade_date,
                        data.get("open"),
                        data.get("high"),
                        data.get("low"),
                        data.get("close"),
                        data.get("volume", 0),
                    )
                    for stock, data in ohlc.items()
                ],
            )

    # ------------------------------------------------------------------
    # EOD snapshot builder (called by ConsensusManager before EOD block)
    # ------------------------------------------------------------------

    async def build_eod_snapshot(self) -> EodSnapshot:
        async with self.pool.acquire() as conn:
            inv_rows = await conn.fetch(
                "SELECT investor_id, cash_balance FROM investors"
            )
            port_rows = await conn.fetch(
                "SELECT investor_id, stock, quantity FROM portfolios WHERE quantity > 0"
            )
            ohlc_rows = await conn.fetch(
                """
                SELECT ph.stock,
                       first_value(ph.clearing_price) OVER w AS open_price,
                       max(ph.clearing_price) OVER w      AS high_price,
                       min(ph.clearing_price) OVER w      AS low_price,
                       last_value(ph.clearing_price) OVER w AS close_price,
                       sum(ph.volume) OVER w              AS total_volume
                FROM price_history ph
                WHERE ph.block_index >= (
                    SELECT COALESCE(MAX(b.index), 0)
                    FROM blocks b WHERE b.is_eod = TRUE
                )
                WINDOW w AS (PARTITION BY ph.stock ORDER BY ph.block_index
                             ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
                """
            )

        portfolios: dict[str, dict] = {}
        for row in inv_rows:
            portfolios[row["investor_id"]] = {
                "cash": float(row["cash_balance"]),
                "shares": {},
            }
        for row in port_rows:
            inv_id = row["investor_id"]
            if inv_id not in portfolios:
                portfolios[inv_id] = {"cash": 0.0, "shares": {}}
            portfolios[inv_id]["shares"][row["stock"]] = row["quantity"]

        seen_stocks: set[str] = set()
        daily_ohlc: dict[str, dict] = {}
        for row in ohlc_rows:
            stock = row["stock"]
            if stock in seen_stocks:
                continue
            seen_stocks.add(stock)
            daily_ohlc[stock] = {
                "open": float(row["open_price"]),
                "high": float(row["high_price"]),
                "low": float(row["low_price"]),
                "close": float(row["close_price"]),
                "volume": int(row["total_volume"]),
            }

        return EodSnapshot(portfolios=portfolios, daily_ohlc=daily_ohlc)

    # ------------------------------------------------------------------
    # Post-block settlement helper
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
    # API query helpers (used by api.py to avoid direct pool access)
    # ------------------------------------------------------------------

    async def get_recent_orders(self, limit: int = 50) -> list[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT order_id, investor_id, bank_id, stock, side, quantity, "
                "limit_price, status, filled_quantity, submitted_at, block_index "
                "FROM orders ORDER BY submitted_at DESC LIMIT $1",
                limit,
            )
        return [dict(r) for r in rows]

    async def get_recent_trades(self, limit: int = 50) -> list[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT trade_id, stock, buyer_bank_id, seller_bank_id, "
                "quantity, price, block_index, traded_at "
                "FROM trades ORDER BY traded_at DESC LIMIT $1",
                limit,
            )
        return [dict(r) for r in rows]

    async def get_investor_portfolio(self, investor_id: str) -> Optional[tuple]:
        inv = await self.get_investor(investor_id)
        if inv is None:
            return None
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT stock, quantity, reserved FROM portfolios "
                "WHERE investor_id=$1 AND quantity > 0",
                investor_id,
            )
        return inv, [dict(r) for r in rows]


def make_database(db_url: str) -> "Database":
    """Factory: returns SqliteDatabase for sqlite:// URLs, Database for postgresql://."""
    if db_url.startswith("sqlite"):
        from .db_sqlite import SqliteDatabase
        return SqliteDatabase(db_url)  # type: ignore[return-value]
    return Database(db_url)
