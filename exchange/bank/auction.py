from __future__ import annotations

import uuid
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from fractions import Fraction
from typing import Optional

from .blockchain import Order, Trade



@dataclass
class AuctionResult:
    trades: list[Trade]
    clearing_prices: dict[str, float]
    unmatched_orders: list[Order]


def run_call_auction(
    orders: list[Order],
    block_index: int = 0,
    last_clearing_prices: Optional[dict[str, float]] = None,
) -> AuctionResult:
    if last_clearing_prices is None:
        last_clearing_prices = {}

    by_stock: dict[str, list[Order]] = defaultdict(list)
    for order in orders:
        by_stock[order.stock].append(order)

    all_trades: list[Trade] = []
    all_clearing_prices: dict[str, float] = {}
    all_unmatched: list[Order] = []

    for stock, stock_orders in by_stock.items():
        trades, price, unmatched = _auction_single_stock(
            stock=stock,
            orders=stock_orders,
            block_index=block_index,
            last_price=last_clearing_prices.get(stock),
        )
        all_trades.extend(trades)
        if price is not None:
            all_clearing_prices[stock] = price
        all_unmatched.extend(unmatched)

    return AuctionResult(
        trades=all_trades,
        clearing_prices=all_clearing_prices,
        unmatched_orders=all_unmatched,
    )


def _auction_single_stock(
    stock: str,
    orders: list[Order],
    block_index: int,
    last_price: Optional[float],
) -> tuple[list[Trade], Optional[float], list[Order]]:
    buys = [o for o in orders if o.side == "buy"]
    sells = [o for o in orders if o.side == "sell"]

    if not buys or not sells:
        return [], None, list(orders)

    buys_sorted = sorted(buys, key=lambda o: (-o.limit_price, o.timestamp))
    sells_sorted = sorted(sells, key=lambda o: (o.limit_price, o.timestamp))

    candidate_prices = sorted({o.limit_price for o in orders})

    best_price: Optional[float] = None
    best_volume = -1

    for p in candidate_prices:
        cum_buy = sum(o.quantity for o in buys if o.limit_price >= p)
        cum_sell = sum(o.quantity for o in sells if o.limit_price <= p)
        matched = min(cum_buy, cum_sell)
        if matched > best_volume:
            best_volume = matched
            best_price = p
        elif matched == best_volume and matched > 0:
            assert best_price is not None
            best_price = _tiebreak(p, best_price, last_price, candidate_prices)

    if best_price is None or best_volume == 0:
        return [], None, list(orders)

    eligible_buys = [o for o in buys_sorted if o.limit_price >= best_price]
    eligible_sells = [o for o in sells_sorted if o.limit_price <= best_price]

    trades, buy_rem, sell_rem = _match_pro_rata(
        eligible_buys, eligible_sells, best_price, block_index
    )

    if not trades:
        return [], None, list(orders)

    remaining = {**buy_rem, **sell_rem}
    unmatched = [o for o in orders if remaining.get(o.order_id, o.quantity) > 0]

    return trades, best_price, unmatched


def _tiebreak(
    candidate: float,
    current_best: float,
    last_price: Optional[float],
    all_prices: list[float],
) -> float:
    if last_price is None:
        # no prior price: use midpoint of the two tied prices.
        # Ties (equidistant from midpoint) keep current_best, matching the
        # tie-break rule used below when last_price is known.
        midpoint = (candidate + current_best) / 2.0
        return min([current_best, candidate], key=lambda p: abs(p - midpoint))
    dist_candidate = abs(candidate - last_price)
    dist_best = abs(current_best - last_price)
    return candidate if dist_candidate < dist_best else current_best


def _match_pro_rata(
    buys: list[Order],
    sells: list[Order],
    price: float,
    block_index: int,
) -> tuple[list[Trade], dict[str, int], dict[str, int]]:
    """
    Emparelha ordens de compra com ordens de venda a um determinado `preço`, utilizando alocação 
    pro-rata quando múltiplos vendedores (ou compradores) apresentam o mesmo preço limite.

    Estratégia:
    - Processa as ordens de compra uma de cada vez (começando pelo preço mais alto).
    - Para cada compra, reúne todas as ordens de venda ainda elegíveis ao preço de compensação 
    e as agrupa por faixa de preço limite (em ordem crescente). Aplica-se a alocação *pro-rata* dentro de cada faixa.
    - Aplica-se uma lógica simétrica, de forma inversa, para o lado da venda.

    Retorna as operações realizadas e a quantidade restante (não executada) por ID de ordem, permitindo distinguir execuções parciais de execuções totais.
    """
    trades: list[Trade] = []

    buy_rem: dict[str, int] = {o.order_id: o.quantity for o in buys}
    sell_rem: dict[str, int] = {o.order_id: o.quantity for o in sells}

    sells_by_price: dict[float, list[Order]] = defaultdict(list)
    for o in sells:
        sells_by_price[o.limit_price].append(o)

    for buy in buys:
        if buy_rem[buy.order_id] == 0:
            continue

        for sell_price in sorted(sells_by_price.keys()):
            if buy_rem[buy.order_id] == 0:
                break

            tier = [
                s for s in sells_by_price[sell_price]
                if sell_rem[s.order_id] > 0 and s.investor_id != buy.investor_id
            ]
            if not tier:
                continue

            tier_supply = sum(sell_rem[s.order_id] for s in tier)
            demand = buy_rem[buy.order_id]
            fill = min(demand, tier_supply)

            if fill == 0:
                continue

            allocations: list[tuple[Order, int]] = []
            total_alloc = 0
            for seller in tier:
                raw = Fraction(fill * sell_rem[seller.order_id], tier_supply)
                alloc = int(raw)
                allocations.append((seller, alloc))
                total_alloc += alloc

            remainder = fill - total_alloc
            for i in range(remainder):
                allocations[i] = (allocations[i][0], allocations[i][1] + 1)

            for seller, qty in allocations:
                if qty == 0:
                    continue
                trade = Trade(
                    trade_id=str(uuid.uuid4()),
                    stock=buy.stock,
                    buyer_order_id=buy.order_id,
                    seller_order_id=seller.order_id,
                    buyer_bank_id=buy.bank_id,
                    seller_bank_id=seller.bank_id,
                    quantity=qty,
                    price=price,
                    block_index=block_index,
                )
                trades.append(trade)
                buy_rem[buy.order_id] -= qty
                sell_rem[seller.order_id] -= qty

    return trades, buy_rem, sell_rem
