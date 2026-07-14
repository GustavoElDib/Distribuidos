from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest

from bank.auction import run_call_auction
from bank.blockchain import Order


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _order(
    side: str,
    qty: int,
    price: float,
    stock: str = "PETR4",
    bank_id: str = "bank_0",
    investor_id: str | None = None,
) -> Order:
    # Default to distinct buyer/seller investor ids: the auction excludes
    # self-trades (same investor_id on both sides), so fixtures that don't
    # care about investor identity must not accidentally collide.
    if investor_id is None:
        investor_id = "inv_buyer" if side == "buy" else "inv_seller"
    return Order(
        order_id=str(uuid.uuid4()),
        investor_id=investor_id,
        bank_id=bank_id,
        stock=stock,
        side=side,
        quantity=qty,
        limit_price=price,
        timestamp=_now(),
    )


def test_basic_match() -> None:
    buy = _order("buy", 100, 10.00)
    sell = _order("sell", 100, 9.00)
    result = run_call_auction([buy, sell], block_index=1)

    assert len(result.trades) == 1
    trade = result.trades[0]
    assert trade.quantity == 100
    # clearing price that maximises volume: both 9.00 and 10.00 match 100 shares
    # tie-break: no prior price → midpoint 9.50 → choose closer = 10.00 or 9.00?
    # midpoint=9.50, |10.00-9.50|=0.50, |9.00-9.50|=0.50 → tie again → min() picks 9.00
    assert trade.price in {9.00, 10.00}
    assert len(result.unmatched_orders) == 0


def test_partial_fill() -> None:
    buy = _order("buy", 100, 10.00)
    sell = _order("sell", 50, 9.00)
    result = run_call_auction([buy, sell], block_index=1)

    assert len(result.trades) == 1
    assert result.trades[0].quantity == 50
    # buy order partially filled → appears in unmatched_orders (remaining 50)
    assert any(o.order_id == buy.order_id for o in result.unmatched_orders)
    assert not any(o.order_id == sell.order_id for o in result.unmatched_orders)


def test_no_match_price_gap() -> None:
    buy = _order("buy", 100, 8.00)
    sell = _order("sell", 100, 10.00)
    result = run_call_auction([buy, sell], block_index=1)

    assert len(result.trades) == 0
    assert len(result.unmatched_orders) == 2


def test_multiple_stocks_independent() -> None:
    orders = [
        _order("buy", 100, 10.00, stock="PETR4"),
        _order("sell", 100, 9.00, stock="PETR4"),
        _order("buy", 200, 5.00, stock="VALE3"),
        _order("sell", 200, 4.00, stock="VALE3"),
    ]
    result = run_call_auction(orders, block_index=1)

    assert "PETR4" in result.clearing_prices
    assert "VALE3" in result.clearing_prices
    petr_trades = [t for t in result.trades if t.stock == "PETR4"]
    vale_trades = [t for t in result.trades if t.stock == "VALE3"]
    assert sum(t.quantity for t in petr_trades) == 100
    assert sum(t.quantity for t in vale_trades) == 200


def test_clearing_price_maximizes_volume() -> None:
    # at P=9.00: buy (qty=200, lim=10), buy (qty=100, lim=9) → cum_buy=300
    #            sell (qty=200, lim=9) → cum_sell=200  → matched=200
    # at P=10.00: cum_buy=200 (only the 10.00 buy), cum_sell=200 → matched=200
    # at P=9.00 matched == 200 same as P=10.00, but try P=8.00:
    # We specifically want P=9.00 to win vs P=10.00.
    # Let's construct so P=9 gives 200 and P=10 gives 100.
    buy_high = _order("buy", 100, 10.00)
    buy_low = _order("buy", 200, 9.00)
    sell = _order("sell", 200, 9.00)

    result = run_call_auction([buy_high, buy_low, sell], block_index=1)
    # P=9.00: cum_buy = 100+200=300, cum_sell=200 → matched=200
    # P=10.00: cum_buy=100, cum_sell=200 → matched=100
    # P=9.00 wins
    assert result.clearing_prices.get("PETR4") == 9.00
    assert sum(t.quantity for t in result.trades) == 200


def test_pro_rata_allocation() -> None:
    sell_a = _order("sell", 100, 9.00, investor_id="inv_a")
    sell_b = _order("sell", 100, 9.00, investor_id="inv_b")
    buy = _order("buy", 100, 10.00, investor_id="inv_c")

    result = run_call_auction([sell_a, sell_b, buy], block_index=1)

    # total matched must be 100
    assert sum(t.quantity for t in result.trades) == 100
    # each seller gets 50 (pro-rata: both have equal supply)
    seller_fills: dict[str, int] = {}
    for t in result.trades:
        seller_fills[t.seller_order_id] = seller_fills.get(t.seller_order_id, 0) + t.quantity
    quantities = sorted(seller_fills.values())
    assert quantities == [50, 50]
