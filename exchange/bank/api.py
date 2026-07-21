from __future__ import annotations

import asyncio
import decimal
import uuid
from datetime import date, datetime, timezone
from typing import TYPE_CHECKING, Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

if TYPE_CHECKING:
    from .node import BankNode

from .blockchain import Order

app = FastAPI(title="Exchange Bank Node API", docs_url="/docs")
_node: "BankNode | None" = None

STOCKS = [
    "PETR4", "VALE3", "ITUB4", "BBDC4", "ABEV3",
    "WEGE3", "RENT3", "BBAS3", "SUZB3", "RDOR3",
    "RADL3", "EGIE3", "LREN3", "HAPV3", "MGLU3",
]


def init_app(node: "BankNode") -> FastAPI:
    global _node
    _node = node
    return app


def _get_node() -> "BankNode":
    if _node is None:
        raise HTTPException(503, "node not initialized")
    return _node


def _serialize(val: Any) -> Any:
    if isinstance(val, decimal.Decimal):
        return float(val)
    if isinstance(val, (datetime, date)):
        return val.isoformat()
    return val


def _row(record) -> dict:
    return {k: _serialize(v) for k, v in record.items()}


# ---------------------------------------------------------------------------
# REST endpoints
# ---------------------------------------------------------------------------

@app.get("/api/status")
async def status():
    node = _get_node()
    pending = await node.gossip_manager.get_pending_orders()
    return {
        "bank_id": node.bank_id,
        "chain_length": len(node.blockchain),
        "connected_peers": list(node.peer_writers.keys()),
        "pending_orders": len(pending),
        "byzantine_nodes": list(node.consensus_manager.byzantine_nodes),
        "bft_n": len(node.consensus_manager._sorted_bank_ids),
        "bft_f": node.consensus_manager.bft_f,
        "bft_quorum": node.consensus_manager.bft_quorum,
        "gossip_mode": "flooding",
    }


@app.get("/api/peers")
async def peers():
    node = _get_node()
    return {"peers": list(node.peer_writers.keys())}


@app.get("/api/blocks")
async def get_blocks(page: int = 1, page_size: int = 10):
    """Blocos paginados, do mais recente para o mais antigo (página 1 = últimos blocos)."""
    node = _get_node()
    chain = node.blockchain
    n = len(chain)
    page_size = max(1, min(page_size, 100))
    total_pages = max(1, -(-n // page_size))
    page = max(1, min(page, total_pages))

    newest = n - 1 - (page - 1) * page_size
    oldest = max(0, newest - page_size + 1)
    result = []
    for i in range(newest, oldest - 1, -1):
        block = chain.get_block(i)
        result.append({
            "index": block.index,
            "timestamp": block.timestamp,
            "producer_id": block.producer_id,
            "orders_count": len(block.orders),
            "trades_count": len(block.trades),
            "block_hash": block.block_hash[:20] + "...",
            "is_eod": block.is_eod,
        })
    return {
        "blocks": result,
        "page": page,
        "page_size": page_size,
        "total_blocks": n,
        "total_pages": total_pages,
    }


@app.get("/api/blocks/{index}")
async def get_block(index: int):
    node = _get_node()
    try:
        block = node.blockchain.get_block(index)
    except IndexError:
        raise HTTPException(404, "block not found")
    return block.to_dict()


class OrderRequest(BaseModel):
    investor_id: str
    stock: str
    side: str
    quantity: int
    limit_price: float


@app.post("/api/orders", status_code=201)
async def submit_order(req: OrderRequest):
    node = _get_node()
    if req.side not in ("buy", "sell"):
        raise HTTPException(400, "side must be 'buy' or 'sell'")
    if req.stock not in STOCKS:
        raise HTTPException(400, f"unknown stock '{req.stock}'")
    if req.quantity <= 0:
        raise HTTPException(400, "quantity must be positive")
    if req.limit_price <= 0:
        raise HTTPException(400, "limit_price must be positive")

    order = Order(
        order_id=str(uuid.uuid4()),
        investor_id=req.investor_id,
        bank_id=node.bank_id,
        stock=req.stock,
        side=req.side,
        quantity=req.quantity,
        limit_price=req.limit_price,
        timestamp=datetime.now(timezone.utc).isoformat(),
    )
    await node.submit_order(order)
    return {"order_id": order.order_id, "status": "submitted", "bank_id": node.bank_id}


@app.get("/api/orders")
async def get_orders(limit: int = 50):
    node = _get_node()
    rows = await node.db.get_recent_orders(limit)
    return {"orders": [_row(r) for r in rows]}


@app.get("/api/portfolio/{investor_id}")
async def get_portfolio(investor_id: str):
    node = _get_node()
    result = await node.db.get_investor_portfolio(investor_id)
    if result is None:
        raise HTTPException(404, "investor not found")
    inv, shares_rows = result
    return {
        "investor_id": investor_id,
        "cash_balance": _serialize(inv["cash_balance"]),
        "cash_reserved": _serialize(inv["cash_reserved"]),
        "shares": {r["stock"]: r["quantity"] for r in shares_rows},
    }


@app.get("/api/trades")
async def get_trades(limit: int = 50):
    node = _get_node()
    rows = await node.db.get_recent_trades(limit)
    return {"trades": [_row(r) for r in rows]}


@app.get("/api/stocks")
async def get_stocks():
    return {"stocks": STOCKS}


@app.get("/api/byzantine")
async def get_byzantine():
    node = _get_node()
    return {
        "byzantine_nodes": list(node.consensus_manager.byzantine_nodes),
        "bft_f": node.consensus_manager.bft_f,
        "bft_quorum": node.consensus_manager.bft_quorum,
    }


@app.get("/api/pending-orders")
async def get_pending_orders():
    node = _get_node()
    pending = await node.gossip_manager.get_pending_orders()
    return {
        "count": len(pending),
        "orders": [o.to_dict() for o in pending],
        "current_leader": node.consensus_manager.get_current_leader(len(node.blockchain)),
        "is_leader": node.consensus_manager.get_current_leader(len(node.blockchain)) == node.bank_id,
    }


@app.post("/api/trigger-block")
async def trigger_block():
    """Força produção imediata de bloco (ignora verificação de liderança)."""
    node = _get_node()
    pending = await node.gossip_manager.get_pending_orders()
    block_index = len(node.blockchain)
    leader = node.consensus_manager.get_current_leader(block_index)
    asyncio.create_task(node._trigger_block_production(force=True))
    return {
        "triggered": True,
        "block_index": block_index,
        "leader": leader,
        "pending_orders": len(pending),
    }


class SeedRequest(BaseModel):
    stock: str = "PETR4"
    quantity: int = 100
    buy_price: float = 36.00
    sell_price: float = 34.00
    pairs: int = 3


@app.post("/api/seed-test")
async def seed_test(req: SeedRequest):
    """Cria pares de ordens compra+venda para testar o leilão."""
    node = _get_node()
    if req.stock not in STOCKS:
        raise HTTPException(400, f"ativo '{req.stock}' desconhecido")
    if req.buy_price <= req.sell_price:
        raise HTTPException(400, "buy_price deve ser maior que sell_price para haver casamento")
    if req.pairs < 1 or req.pairs > 20:
        raise HTTPException(400, "pairs deve ser entre 1 e 20")

    now = datetime.now(timezone.utc).isoformat()
    created = []
    for i in range(req.pairs):
        buy = Order(
            order_id=str(uuid.uuid4()),
            investor_id=f"INV_BUY_{i:03d}",
            bank_id=node.bank_id,
            stock=req.stock,
            side="buy",
            quantity=req.quantity,
            limit_price=req.buy_price,
            timestamp=now,
        )
        sell = Order(
            order_id=str(uuid.uuid4()),
            investor_id=f"INV_SELL_{i:03d}",
            bank_id=node.bank_id,
            stock=req.stock,
            side="sell",
            quantity=req.quantity,
            limit_price=req.sell_price,
            timestamp=now,
        )
        await node.submit_order(buy)
        await node.submit_order(sell)
        created.append({"buy": buy.order_id, "sell": sell.order_id})

    return {
        "created_pairs": req.pairs,
        "stock": req.stock,
        "buy_price": req.buy_price,
        "sell_price": req.sell_price,
        "clearing_price_expected": req.sell_price,
        "orders": created,
    }


@app.get("/api/auction-status")
async def auction_status():
    """Timer do leilão controlado pelo líder."""
    node = _get_node()
    return node.get_auction_status()


@app.get("/api/vote-status")
async def vote_status():
    """Estado ao vivo da rodada de votação atual ou última."""
    node = _get_node()
    return node.consensus_manager.get_vote_status()


@app.get("/api/pending-vote")
async def pending_vote():
    """Bloco candidato aguardando a decisão manual do gestor deste banco.

    Retornado quando outro banco (o líder) propõe um bloco e este banco
    precisa que seu gestor aprove ou rejeite antes de emitir o voto assinado.
    """
    node = _get_node()
    pending = node.get_pending_manager_vote()
    if pending is None:
        return {"pending": False}
    return {"pending": True, "candidate": pending, "bank_id": node.bank_id}


class CastVoteRequest(BaseModel):
    approve: bool


@app.post("/api/cast-vote")
async def cast_vote(req: CastVoteRequest):
    """O gestor do banco registra seu voto (aprovar/rejeitar) no bloco pendente."""
    node = _get_node()
    ok = node.submit_manager_vote(req.approve)
    if not ok:
        raise HTTPException(409, "nenhum bloco aguardando votação no momento")
    return {
        "recorded": True,
        "approved": req.approve,
        "bank_id": node.bank_id,
    }


# ---------------------------------------------------------------------------
# HTML dashboard
# ---------------------------------------------------------------------------

_HTML = """<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Exchange Bank Node</title>
  <style>
    *{box-sizing:border-box;margin:0;padding:0}
    :root{
      --bg:#0f1117;--surface:#1a1d2e;--surface2:#242740;
      --accent:#5c7cfa;--accent2:#748ffc;--green:#51cf66;
      --red:#ff6b6b;--yellow:#ffd43b;--orange:#ff922b;--text:#e9ecef;
      --muted:#868e96;--border:#2e3250;
    }
    body{background:var(--bg);color:var(--text);font-family:'Segoe UI',system-ui,sans-serif;font-size:14px;line-height:1.5}
    header{background:var(--surface);border-bottom:1px solid var(--border);padding:12px 24px;display:flex;align-items:center;gap:16px}
    header h1{font-size:18px;font-weight:600;color:var(--accent2)}
    .badge{display:inline-block;padding:2px 10px;border-radius:12px;font-size:12px;font-weight:600}
    .badge-green{background:#1a3a22;color:var(--green)}
    .badge-red{background:#3a1a1a;color:var(--red)}
    .badge-blue{background:#1a2a4a;color:var(--accent2)}
    .badge-yellow{background:#3a3010;color:var(--yellow)}
    .updated{margin-left:auto;font-size:12px;color:var(--muted)}
    main{padding:20px 24px;max-width:1400px;margin:0 auto}
    .grid-4{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:20px}
    .card{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:16px}
    .card-label{font-size:11px;text-transform:uppercase;letter-spacing:.06em;color:var(--muted);margin-bottom:6px}
    .card-value{font-size:28px;font-weight:700;color:var(--accent2)}
    .card-sub{font-size:12px;color:var(--muted);margin-top:4px}
    .section{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:20px;margin-bottom:20px}
    .section h2{font-size:15px;font-weight:600;margin-bottom:16px;color:var(--text)}
    .grid-2{display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-bottom:20px}
    table{width:100%;border-collapse:collapse}
    th{text-align:left;padding:8px 12px;font-size:11px;text-transform:uppercase;letter-spacing:.05em;color:var(--muted);border-bottom:1px solid var(--border)}
    td{padding:8px 12px;border-bottom:1px solid var(--border);font-size:13px}
    tr:last-child td{border-bottom:none}
    tr:hover td{background:var(--surface2)}
    .form-row{display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end}
    .form-group{display:flex;flex-direction:column;gap:4px}
    .form-group label{font-size:12px;color:var(--muted)}
    input,select{background:var(--surface2);border:1px solid var(--border);color:var(--text);padding:7px 10px;border-radius:6px;font-size:13px;outline:none}
    input:focus,select:focus{border-color:var(--accent)}
    select option{background:var(--surface2)}
    .btn{padding:8px 20px;border-radius:6px;border:none;cursor:pointer;font-size:13px;font-weight:600;transition:.15s}
    .btn-primary{background:var(--accent);color:#fff}
    .btn-primary:hover{background:var(--accent2)}
    .btn-orange{background:#c05621;color:#fff}
    .btn-orange:hover{background:var(--orange)}
    .btn-green{background:#1a6b30;color:#fff}
    .btn-green:hover{background:var(--green);color:#000}
    .btn-sm{padding:5px 14px;font-size:12px}
    .btn:disabled{opacity:.5;cursor:not-allowed}
    .action-bar{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:16px;align-items:center}
    .leader-badge{padding:4px 12px;border-radius:8px;font-size:12px;background:#2a1f00;border:1px solid #5a4000;color:var(--yellow)}
    .peer-list{display:flex;flex-wrap:wrap;gap:8px}
    .peer-chip{padding:4px 12px;border-radius:16px;font-size:12px;background:var(--surface2);border:1px solid var(--border);display:flex;align-items:center;gap:6px}
    .dot{width:8px;height:8px;border-radius:50%;flex-shrink:0}
    .dot-green{background:var(--green)}
    .dot-red{background:var(--red)}
    .bft-info{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:12px}
    .bft-item{text-align:center;padding:10px;background:var(--surface2);border-radius:8px}
    .bft-val{font-size:22px;font-weight:700;color:var(--accent2)}
    .bft-lbl{font-size:11px;color:var(--muted);margin-top:2px}
    #msg{padding:8px 14px;border-radius:6px;font-size:13px;margin-top:10px;display:none}
    .msg-ok{background:#1a3a22;color:var(--green);border:1px solid #2a5a32}
    .msg-err{background:#3a1a1a;color:var(--red);border:1px solid #5a2a2a}
    .msg-warn{background:#3a2a00;color:var(--yellow);border:1px solid #5a4a00}
    .hash{font-family:monospace;font-size:12px;color:var(--muted)}
    .toast{position:fixed;top:20px;right:20px;padding:12px 20px;border-radius:10px;font-size:13px;font-weight:600;z-index:9999;opacity:0;transform:translateX(40px);transition:.3s;pointer-events:none}
    .toast.show{opacity:1;transform:translateX(0)}
    .toast-ok{background:#1a3a22;color:var(--green);border:1px solid #2a5a32}
    .toast-info{background:#1a2a4a;color:var(--accent2);border:1px solid #2a3a6a}
    .status-pending{color:var(--yellow)}
    .status-matched{color:var(--green)}
    .status-expired{color:var(--muted)}
    .pending-order{display:flex;align-items:center;gap:8px;padding:6px 10px;background:var(--surface2);border-radius:6px;font-size:12px;margin-bottom:4px}
    .po-side-buy{color:var(--green);font-weight:700}
    .po-side-sell{color:var(--red);font-weight:700}
    .progress-bar{height:4px;background:var(--surface2);border-radius:2px;margin:8px 0;overflow:hidden}
    .progress-fill{height:100%;background:var(--accent);width:0%;transition:width .5s}
    ::-webkit-scrollbar{width:6px;height:6px}
    ::-webkit-scrollbar-track{background:var(--bg)}
    ::-webkit-scrollbar-thumb{background:var(--border);border-radius:3px}
    .navlink{color:var(--muted);text-decoration:none;font-size:13px;padding:4px 10px;border-radius:6px;border:1px solid var(--border)}
    .navlink:hover{color:var(--text);border-color:var(--accent)}
    .pager{display:flex;align-items:center;justify-content:center;gap:14px;margin-top:12px}
    .pager-info{font-size:12px;color:var(--muted);min-width:170px;text-align:center}
    /* Vote modal */
    .modal-overlay{position:fixed;inset:0;background:rgba(0,0,0,.75);z-index:10000;display:none;align-items:center;justify-content:center;backdrop-filter:blur(3px)}
    .modal-overlay.show{display:flex;animation:fadein .2s}
    @keyframes fadein{from{opacity:0}to{opacity:1}}
    @keyframes pulse{0%{box-shadow:0 0 0 0 rgba(255,146,43,.5)}70%{box-shadow:0 0 0 16px rgba(255,146,43,0)}100%{box-shadow:0 0 0 0 rgba(255,146,43,0)}}
    .modal{background:var(--surface);border:2px solid var(--orange);border-radius:16px;padding:0;width:560px;max-width:94vw;max-height:92vh;overflow-y:auto;animation:pulse 2s infinite}
    .modal-head{padding:20px 24px;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:12px}
    .modal-head h2{font-size:19px;color:var(--orange);margin:0}
    .modal-body{padding:20px 24px}
    .modal-foot{padding:16px 24px;border-top:1px solid var(--border);display:flex;gap:12px}
    .modal-foot .btn{flex:1;padding:14px;font-size:15px}
    .btn-approve{background:var(--green);color:#04210d}
    .btn-approve:hover{background:#69db7c}
    .btn-reject{background:var(--red);color:#2a0808}
    .btn-reject:hover{background:#ff8787}
    .vote-timer{font-size:34px;font-weight:800;color:var(--yellow);font-variant-numeric:tabular-nums;text-align:center}
    .kv{display:flex;justify-content:space-between;padding:7px 0;border-bottom:1px solid var(--border);font-size:14px}
    .kv:last-child{border-bottom:none}
    .kv b{color:var(--accent2)}
    .rec-accept{background:#1a3a22;color:var(--green);border:1px solid #2a5a32;padding:8px 12px;border-radius:8px;font-size:13px;text-align:center;margin-top:10px}
    .rec-reject{background:#3a1a1a;color:var(--red);border:1px solid #5a2a2a;padding:8px 12px;border-radius:8px;font-size:13px;text-align:center;margin-top:10px}
    .trade-mini{font-size:12px;padding:5px 10px;background:var(--surface2);border-radius:6px;margin-bottom:4px;display:flex;justify-content:space-between}
  </style>
</head>
<body>
<div id="toast" class="toast toast-ok"></div>

<!-- Popup de votação do gestor -->
<div id="vote-modal" class="modal-overlay">
  <div class="modal">
    <div class="modal-head">
      <span style="font-size:28px">🗳️</span>
      <div>
        <h2>Novo Bloco para Votação</h2>
        <div style="font-size:12px;color:var(--muted)">O líder propôs um bloco. Como gestor, aprove ou rejeite a inclusão na blockchain.</div>
      </div>
    </div>
    <div class="modal-body">
      <div class="vote-timer" id="vote-modal-timer">75</div>
      <div style="text-align:center;font-size:11px;color:var(--muted);margin-bottom:16px">segundos para decidir (senão usa a verificação automática)</div>
      <div class="kv"><span>Bloco #</span><b id="vm-index">—</b></div>
      <div class="kv"><span>Proposto por (líder)</span><b id="vm-producer">—</b></div>
      <div class="kv"><span>Ordens no bloco</span><b id="vm-orders">—</b></div>
      <div class="kv"><span>Trades gerados no leilão</span><b id="vm-trades">—</b></div>
      <div class="kv"><span>Hash do bloco</span><b id="vm-hash" style="font-family:monospace;font-size:11px">—</b></div>
      <div id="vm-tradelist" style="margin-top:12px"></div>
      <div id="vm-rec"></div>
    </div>
    <div class="modal-foot">
      <button class="btn btn-reject" onclick="castVote(false)">❌ Rejeitar Bloco</button>
      <button class="btn btn-approve" onclick="castVote(true)">✅ Aprovar Bloco</button>
    </div>
  </div>
</div>

<header>
  <h1>Painel do Gestor &mdash; <span id="hdr-bank">...</span></h1>
  <span class="badge badge-blue">Flooding</span>
  <span class="badge badge-green" id="hdr-status">Online</span>
  <span id="leader-badge" class="leader-badge" style="display:none">⭐ Líder atual</span>
  <a href="/cliente" class="navlink" target="_blank">🧑‍💼 Abrir painel do Cliente</a>
  <span class="updated">Atualizado: <span id="hdr-time">—</span></span>
</header>

<main>
  <!-- Cards de status -->
  <div class="grid-4">
    <div class="card">
      <div class="card-label">Blocos na Chain</div>
      <div class="card-value" id="c-chain">—</div>
      <div class="card-sub">blocos confirmados</div>
    </div>
    <div class="card">
      <div class="card-label">Peers Conectados</div>
      <div class="card-value" id="c-peers">—</div>
      <div class="card-sub" id="c-peers-sub">aguardando...</div>
    </div>
    <div class="card">
      <div class="card-label">Ordens Pendentes</div>
      <div class="card-value" id="c-pending" style="color:var(--yellow)">—</div>
      <div class="card-sub">aguardando leilão</div>
    </div>
    <div class="card">
      <div class="card-label">Nós Bizantinos</div>
      <div class="card-value" id="c-byz">0</div>
      <div class="card-sub" id="c-byz-ids">nenhum detectado</div>
    </div>
  </div>

  <!-- Timer do leilão + controles -->
  <div class="grid-2" style="margin-bottom:0">
    <div class="section" style="margin-bottom:0">
      <h2>Timer do Leilão <span id="leader-crown" style="display:none;font-size:13px;color:var(--yellow)">★ Você é o Líder</span></h2>
      <div style="display:flex;align-items:center;gap:20px;margin-bottom:14px;flex-wrap:wrap">
        <div style="text-align:center">
          <div id="auction-countdown" style="font-size:48px;font-weight:700;color:var(--yellow);font-variant-numeric:tabular-nums;letter-spacing:-2px">—:——</div>
          <div style="font-size:11px;color:var(--muted)">até o próximo leilão</div>
        </div>
        <div style="flex:1;min-width:180px">
          <div class="progress-bar" style="height:10px;margin-bottom:6px">
            <div class="progress-fill" id="auction-bar" style="background:var(--yellow)"></div>
          </div>
          <div style="font-size:12px;color:var(--muted)">
            Líder: <strong id="auction-leader" style="color:var(--accent2)">—</strong>
            &nbsp;|&nbsp; Intervalo: <span id="auction-interval">—</span>s
          </div>
          <div style="font-size:11px;color:var(--muted);margin-top:4px" id="auction-note">
            O líder fecha a janela e produz o bloco automaticamente ao fim do timer.
          </div>
        </div>
      </div>
      <div class="action-bar">
        <button id="btn-trigger" class="btn btn-orange" onclick="triggerBlock()">⚡ Forçar Leilão Agora</button>
        <button id="btn-seed" class="btn btn-green" onclick="openSeed()">🌱 Ordens de Teste</button>
        <span style="font-size:11px;color:var(--muted)" id="trigger-log"></span>
      </div>
      <div id="seed-panel" style="display:none;margin-top:12px;padding:14px;background:var(--surface2);border-radius:8px">
        <div style="font-size:12px;color:var(--muted);margin-bottom:10px">
          Cria pares compra+venda com preços sobrepostos para garantir execução de trades.
        </div>
        <div class="form-row">
          <div class="form-group">
            <label>Ativo</label>
            <select id="seed-stock"><option>PETR4</option><option>VALE3</option><option>ITUB4</option><option>BBDC4</option><option>ABEV3</option></select>
          </div>
          <div class="form-group">
            <label>P. Compra (R$)</label>
            <input type="number" id="seed-buy" value="36.00" step="0.01" style="width:100px"/>
          </div>
          <div class="form-group">
            <label>P. Venda (R$)</label>
            <input type="number" id="seed-sell" value="34.00" step="0.01" style="width:100px"/>
          </div>
          <div class="form-group">
            <label>Qtd</label>
            <input type="number" id="seed-qty" value="100" style="width:80px"/>
          </div>
          <div class="form-group">
            <label>Pares</label>
            <input type="number" id="seed-pairs" value="2" min="1" max="10" style="width:65px"/>
          </div>
          <div class="form-group">
            <label>&nbsp;</label>
            <button class="btn btn-green" onclick="runSeed()">Criar e Disparar</button>
          </div>
        </div>
        <div id="seed-msg" style="display:none;margin-top:8px;padding:7px 12px;border-radius:6px;font-size:12px"></div>
      </div>
    </div>

    <!-- Painel de votação dos gestores -->
    <div class="section" style="margin-bottom:0" id="vote-section">
      <h2>Votação dos Gestores <span id="vote-status-badge" style="font-size:12px;font-weight:400;color:var(--muted)"></span></h2>
      <div id="vote-panel">
        <div style="color:var(--muted);font-size:13px;text-align:center;padding:20px 0">
          Aguardando próxima rodada de votação...
        </div>
      </div>
      <div id="vote-result" style="display:none;margin-top:12px;padding:10px 14px;border-radius:8px;font-size:13px"></div>
      <div style="font-size:11px;color:var(--muted);margin-top:10px">
        Cada banco verifica o bloco candidato e assina seu voto com Ed25519.
        Quórum BFT (2f+1) necessário para confirmar o bloco.
      </div>
    </div>
  </div>

  <!-- BFT + Peers (segunda linha) -->
  <div class="grid-2">
    <div class="section">
      <h2>Tolerância Bizantina (BFT)</h2>
      <div class="bft-info">
        <div class="bft-item"><div class="bft-val" id="bft-n">—</div><div class="bft-lbl">Bancos (n)</div></div>
        <div class="bft-item"><div class="bft-val" id="bft-f">—</div><div class="bft-lbl">Falhas (f)</div></div>
        <div class="bft-item"><div class="bft-val" id="bft-q">—</div><div class="bft-lbl">Quórum (2f+1)</div></div>
      </div>
      <div id="vote-progress" style="display:none">
        <div style="font-size:12px;color:var(--muted);margin-bottom:4px">Votação em andamento...</div>
        <div class="progress-bar"><div class="progress-fill" id="vote-bar"></div></div>
        <div style="font-size:11px;color:var(--muted)" id="vote-count">0 votos</div>
      </div>
    </div>
    <div class="section">
      <h2>Peers Conectados</h2>
      <div class="peer-list" id="peer-list">
        <span style="color:var(--muted);font-size:13px">Carregando...</span>
      </div>
    </div>
  </div>

  <!-- Ordens pendentes -->
  <div class="section">
    <h2>Ordens Pendentes <span id="pending-count-badge" style="font-size:12px;color:var(--muted);font-weight:400"></span></h2>
    <div id="pending-list" style="max-height:160px;overflow-y:auto">
      <span style="color:var(--muted);font-size:13px">Nenhuma ordem pendente.</span>
    </div>
    <div style="font-size:11px;color:var(--muted);margin-top:8px">
      ⚠ O leilão só executa trades quando há ordens de <strong>compra E venda</strong> do mesmo ativo com preços sobrepostos.
      Use "Criar Ordens de Teste" para gerar pares compatíveis.
      <strong>Um novo bloco só é criado se o leilão gerar pelo menos 1 trade</strong> — sem casamento, as ordens permanecem pendentes para o próximo leilão.
    </div>
  </div>

  <!-- Submissão manual -->
  <div class="section">
    <h2>Submeter Ordem Individual</h2>
    <form id="order-form">
      <div class="form-row">
        <div class="form-group">
          <label>Investidor</label>
          <input type="text" id="f-investor" placeholder="INV001" required style="width:120px"/>
        </div>
        <div class="form-group">
          <label>Ativo</label>
          <select id="f-stock">
            <option>PETR4</option><option>VALE3</option><option>ITUB4</option>
            <option>BBDC4</option><option>ABEV3</option><option>WEGE3</option>
            <option>RENT3</option><option>BBAS3</option><option>SUZB3</option>
            <option>RDOR3</option><option>RADL3</option><option>EGIE3</option>
            <option>LREN3</option><option>HAPV3</option><option>MGLU3</option>
          </select>
        </div>
        <div class="form-group">
          <label>Lado</label>
          <select id="f-side">
            <option value="buy">Compra</option>
            <option value="sell">Venda</option>
          </select>
        </div>
        <div class="form-group">
          <label>Quantidade</label>
          <input type="number" id="f-qty" min="1" value="100" required style="width:90px"/>
        </div>
        <div class="form-group">
          <label>Preço Limite (R$)</label>
          <input type="number" id="f-price" min="0.01" step="0.01" value="35.00" required style="width:110px"/>
        </div>
        <div class="form-group">
          <label>&nbsp;</label>
          <button type="submit" class="btn btn-primary">Enviar Ordem</button>
        </div>
      </div>
      <div id="msg" style="margin-top:10px;padding:8px 14px;border-radius:6px;font-size:13px;display:none"></div>
    </form>
  </div>

  <!-- Blocos -->
  <div class="section">
    <h2>Blocos da Chain <span id="blocks-total" style="font-size:12px;color:var(--muted);font-weight:400"></span></h2>
    <div style="overflow-x:auto">
    <table>
      <thead>
        <tr><th>#</th><th>Horário</th><th>Produtor</th><th>Ordens</th><th>Trades</th><th>Hash</th><th>EOD</th></tr>
      </thead>
      <tbody id="blocks-body">
        <tr><td colspan="7" style="color:var(--muted);text-align:center">Nenhum bloco ainda</td></tr>
      </tbody>
    </table>
    </div>
    <div class="pager">
      <button class="btn btn-sm btn-primary" id="blk-prev" onclick="changeBlockPage(-1)" disabled>&laquo; Mais recentes</button>
      <span class="pager-info" id="blk-pageinfo">Página 1 de 1</span>
      <button class="btn btn-sm btn-primary" id="blk-next" onclick="changeBlockPage(1)" disabled>Mais antigos &raquo;</button>
    </div>
  </div>

  <!-- Trades -->
  <div class="section">
    <h2>Trades Executados</h2>
    <div style="overflow-x:auto">
    <table>
      <thead>
        <tr><th>Ativo</th><th>Qtd</th><th>Preço</th><th>Banco Comprador</th><th>Banco Vendedor</th><th>Bloco</th><th>Horário</th></tr>
      </thead>
      <tbody id="trades-body">
        <tr><td colspan="7" style="color:var(--muted);text-align:center">Nenhum trade ainda</td></tr>
      </tbody>
    </table>
    </div>
  </div>
</main>

<script>
const $ = id => document.getElementById(id);
let prevChain = 0;
let prevPending = 0;

function fmt(ts) {
  if (!ts) return '—';
  return new Date(ts).toLocaleTimeString('pt-BR');
}

function showToast(msg, type='ok') {
  const t = $('toast');
  t.textContent = msg;
  t.className = `toast toast-${type} show`;
  setTimeout(() => { t.className = `toast toast-${type}`; }, 3500);
}

function showMsg(id, text, cls) {
  const el = $(id);
  el.textContent = text;
  el.className = cls;
  el.style.display = 'block';
  setTimeout(() => { el.style.display = 'none'; }, 5000);
}

async function loadStatus() {
  try {
    const r = await fetch('/api/status');
    const d = await r.json();
    $('hdr-bank').textContent = d.bank_id;
    $('c-chain').textContent = d.chain_length;
    $('c-peers').textContent = d.connected_peers.length;
    $('c-peers-sub').textContent = `de ${(d.bft_n||1)-1} esperados`;
    $('c-pending').textContent = d.pending_orders;
    $('c-byz').textContent = d.byzantine_nodes.length;
    $('c-byz').style.color = d.byzantine_nodes.length > 0 ? 'var(--red)' : 'var(--green)';
    $('c-byz-ids').textContent = d.byzantine_nodes.length > 0
      ? d.byzantine_nodes.join(', ') : 'nenhum detectado';
    $('bft-n').textContent = d.bft_n || '—';
    $('bft-f').textContent = d.bft_f ?? '—';
    $('bft-q').textContent = d.bft_quorum || '—';
    $('hdr-time').textContent = new Date().toLocaleTimeString('pt-BR');

    // notificação de novo bloco
    if (prevChain > 0 && d.chain_length > prevChain) {
      showToast(`✅ Bloco #${d.chain_length - 1} confirmado!`, 'ok');
    }
    prevChain = d.chain_length;

    // peers
    const pl = $('peer-list');
    pl.innerHTML = d.connected_peers.length === 0
      ? '<span style="color:var(--muted);font-size:13px">Nenhum peer conectado</span>'
      : d.connected_peers.map(p =>
          `<span class="peer-chip"><span class="dot dot-green"></span>${p}</span>`
        ).join('');
  } catch(e) {
    $('hdr-status').textContent = 'Offline';
    $('hdr-status').className = 'badge badge-red';
  }
}

async function loadPending() {
  try {
    const r = await fetch('/api/pending-orders');
    const d = await r.json();
    const list = $('pending-list');
    const badge = $('pending-count-badge');

    // atualiza líder info
    $('leader-info').textContent = `Líder do próximo bloco: ${d.current_leader}`;
    const lb = $('leader-badge');
    lb.style.display = d.is_leader ? 'inline-block' : 'none';

    // notificação de ordens sendo processadas
    if (prevPending > 0 && d.count === 0 && prevPending > 0) {
      showToast('📦 Ordens processadas no último bloco!', 'info');
    }
    prevPending = d.count;

    badge.textContent = d.count > 0 ? `(${d.count})` : '';
    if (d.orders.length === 0) {
      list.innerHTML = '<span style="color:var(--muted);font-size:13px">Nenhuma ordem pendente.</span>';
      return;
    }

    // contagem por lado
    const buys  = d.orders.filter(o => o.side === 'buy').length;
    const sells = d.orders.filter(o => o.side === 'sell').length;
    const pairs = Math.min(buys, sells);
    const summary = `<div style="font-size:12px;margin-bottom:8px;color:var(--muted)">
      <span class="po-side-buy">▲ ${buys} compras</span> &nbsp;
      <span class="po-side-sell">▼ ${sells} vendas</span> &nbsp;
      ${pairs > 0
        ? `<span style="color:var(--green)">✓ ${pairs} par(es) possivelmente casáveis</span>`
        : `<span style="color:var(--red)">⚠ Sem pares — adicione ordens do lado oposto</span>`}
    </div>`;

    const items = d.orders.slice(0, 20).map(o => `
      <div class="pending-order">
        <span class="${o.side==='buy'?'po-side-buy':'po-side-sell'}">${o.side==='buy'?'COMPRA':'VENDA'}</span>
        <strong>${o.stock}</strong>
        <span>${o.quantity} ações</span>
        <span>@ R$ ${Number(o.limit_price).toFixed(2)}</span>
        <span style="color:var(--muted);font-size:11px">${o.investor_id}</span>
      </div>`).join('');

    list.innerHTML = summary + items;
  } catch(e) {}
}

const BLOCKS_PAGE_SIZE = 10;
let blocksPage = 1;
let blocksTotalPages = 1;

async function loadBlocks() {
  try {
    const r = await fetch(`/api/blocks?page=${blocksPage}&page_size=${BLOCKS_PAGE_SIZE}`);
    const d = await r.json();
    blocksPage = d.page;
    blocksTotalPages = d.total_pages;
    $('blk-pageinfo').textContent = `Página ${d.page} de ${d.total_pages}`;
    $('blocks-total').textContent = `(${d.total_blocks} bloco${d.total_blocks === 1 ? '' : 's'})`;
    $('blk-prev').disabled = d.page <= 1;
    $('blk-next').disabled = d.page >= d.total_pages;
    const tbody = $('blocks-body');
    if (!d.blocks.length) return;
    tbody.innerHTML = d.blocks.map(b => `
      <tr>
        <td><strong>#${b.index}</strong></td>
        <td>${fmt(b.timestamp)}</td>
        <td><span class="badge badge-blue">${b.producer_id}</span></td>
        <td>${b.orders_count}</td>
        <td>${b.trades_count > 0 ? `<span style="color:var(--green)">${b.trades_count}</span>` : '0'}</td>
        <td class="hash">${b.block_hash}</td>
        <td>${b.is_eod ? '<span class="badge badge-yellow">EOD</span>' : ''}</td>
      </tr>`).join('');
  } catch(e) {}
}

function changeBlockPage(delta) {
  blocksPage = Math.min(Math.max(1, blocksPage + delta), blocksTotalPages);
  loadBlocks();
}

async function loadTrades() {
  try {
    const r = await fetch('/api/trades?limit=20');
    const d = await r.json();
    const tbody = $('trades-body');
    if (!d.trades.length) return;
    tbody.innerHTML = d.trades.map(t => `
      <tr>
        <td><strong>${t.stock}</strong></td>
        <td>${t.quantity}</td>
        <td><strong>R$ ${Number(t.price).toFixed(2)}</strong></td>
        <td>${t.buyer_bank_id}</td>
        <td>${t.seller_bank_id}</td>
        <td>#${t.block_index}</td>
        <td>${fmt(t.traded_at)}</td>
      </tr>`).join('');
  } catch(e) {}
}

async function triggerBlock() {
  const btn = $('btn-trigger');
  btn.disabled = true;
  btn.textContent = '⏳ Aguardando consenso...';
  try {
    const r = await fetch('/api/trigger-block', {method:'POST'});
    const d = await r.json();
    $('trigger-log').textContent =
      `⚡ Leilão disparado às ${new Date().toLocaleTimeString('pt-BR')} — bloco #${d.block_index} com ${d.pending_orders} ordem(ns) | líder: ${d.leader}`;
    showToast('⚡ Leilão iniciado! Aguarde o bloco ser confirmado...', 'info');
    setTimeout(refresh, 3000);
    setTimeout(refresh, 8000);
  } catch(e) {
    $('trigger-log').textContent = 'Erro ao disparar leilão.';
  }
  setTimeout(() => { btn.disabled=false; btn.textContent='⚡ Disparar Leilão Agora'; }, 5000);
}

function openSeed() {
  const p = $('seed-panel');
  p.style.display = p.style.display === 'none' ? 'block' : 'none';
}

async function runSeed() {
  const payload = {
    stock: $('seed-stock').value,
    buy_price: parseFloat($('seed-buy').value),
    sell_price: parseFloat($('seed-sell').value),
    quantity: parseInt($('seed-qty').value),
    pairs: parseInt($('seed-pairs').value),
  };
  try {
    const r = await fetch('/api/seed-test', {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify(payload),
    });
    const d = await r.json();
    if (r.ok) {
      showMsg('seed-msg',
        `✓ ${d.created_pairs} par(es) criados para ${d.stock} — preço esperado: R$ ${Number(d.sell_price).toFixed(2)}`,
        'msg-ok');
      showToast('🌱 Ordens criadas! Disparando leilão...', 'ok');
      setTimeout(triggerBlock, 800);
    } else {
      showMsg('seed-msg', `Erro: ${d.detail}`, 'msg-err');
    }
  } catch(e) {
    showMsg('seed-msg', 'Erro de conexão.', 'msg-err');
  }
}

$('order-form').addEventListener('submit', async e => {
  e.preventDefault();
  const payload = {
    investor_id: $('f-investor').value.trim(),
    stock: $('f-stock').value,
    side: $('f-side').value,
    quantity: parseInt($('f-qty').value),
    limit_price: parseFloat($('f-price').value),
  };
  try {
    const r = await fetch('/api/orders', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify(payload),
    });
    const d = await r.json();
    if (r.ok) {
      showMsg('msg', `✓ Ordem enviada: ${payload.side==='buy'?'COMPRA':'VENDA'} ${payload.quantity} ${payload.stock} @ R$ ${payload.limit_price} — ID: ${d.order_id.slice(0,8)}...`, 'msg-ok');
      showToast('✓ Ordem propagada via flooding para todos os bancos', 'info');
    } else {
      showMsg('msg', `Erro: ${d.detail}`, 'msg-err');
    }
  } catch(e) {
    showMsg('msg', 'Erro de conexão.', 'msg-err');
  }
});

// ---- Auction timer ----
function fmtCountdown(secs) {
  const m = Math.floor(secs / 60);
  const s = Math.floor(secs % 60);
  return `${m}:${String(s).padStart(2,'0')}`;
}

async function loadAuction() {
  try {
    const r = await fetch('/api/auction-status');
    const d = await r.json();
    const rem = d.remaining_seconds;
    const total = d.auction_interval_seconds;
    $('auction-countdown').textContent = fmtCountdown(rem);
    $('auction-countdown').style.color = rem < 30 ? 'var(--red)' : rem < 60 ? 'var(--orange)' : 'var(--yellow)';
    $('auction-bar').style.width = (((total - rem) / total) * 100).toFixed(1) + '%';
    $('auction-leader').textContent = d.current_leader;
    $('auction-interval').textContent = total;
    const crown = $('leader-crown');
    crown.style.display = d.is_leader ? 'inline' : 'none';
    $('auction-note').textContent = d.is_leader
      ? '★ Este banco é o líder — vai fechar a janela e propor o bloco.'
      : `Aguardando o líder ${d.current_leader} fechar a janela de ordens.`;
  } catch(e) {}
}

// ---- Vote panel ----
const VOTE_COLORS = {
  pending:   { bg:'var(--surface2)', border:'var(--border)', icon:'⏳', label:'Aguardando' },
  accepted:  { bg:'#1a3a22',        border:'var(--green)',  icon:'✅', label:'ACEITO' },
  rejected:  { bg:'#3a1a1a',        border:'var(--red)',    icon:'❌', label:'REJEITADO' },
  timeout:   { bg:'#2a2a1a',        border:'var(--yellow)', icon:'⏰', label:'Timeout' },
  byzantine: { bg:'#3a1a2a',        border:'#c05621',       icon:'⚠️', label:'Byzantino' },
};

let lastVoteBlock = null;

async function loadVotes() {
  try {
    const r = await fetch('/api/vote-status');
    const d = await r.json();
    const panel = $('vote-panel');
    const badge = $('vote-status-badge');
    const resultEl = $('vote-result');

    if (d.active) {
      badge.textContent = `— Bloco #${d.block_index} em votação (${d.elapsed_seconds}s)`;
      badge.style.color = 'var(--orange)';
      resultEl.style.display = 'none';
      panel.innerHTML = buildVoteCards(d.votes, null);
    } else if (d.last_result) {
      const res = d.last_result;
      // Show final result if changed
      if (lastVoteBlock !== res.block_index) {
        lastVoteBlock = res.block_index;
        showToast(
          res.accepted
            ? `✅ Bloco #${res.block_index} APROVADO por consenso (${res.accept_count}/${res.quorum} votos)`
            : `❌ Bloco #${res.block_index} REJEITADO (${res.accept_count}/${res.quorum} aceites, precisava ${res.quorum})`,
          res.accepted ? 'ok' : 'info'
        );
      }
      badge.textContent = `— última votação: Bloco #${res.block_index}`;
      badge.style.color = 'var(--muted)';
      panel.innerHTML = buildVoteCards(res.votes, res);
      // show summary
      resultEl.style.display = 'block';
      resultEl.style.background = res.accepted ? '#1a3a22' : '#3a1a1a';
      resultEl.style.border = `1px solid ${res.accepted ? 'var(--green)' : 'var(--red)'}`;
      resultEl.innerHTML = res.accepted
        ? `<strong style="color:var(--green)">✅ Bloco #${res.block_index} APROVADO</strong> — ${res.accept_count} aceites de ${res.quorum} necessários (quórum atingido)`
        : `<strong style="color:var(--red)">❌ Bloco #${res.block_index} REJEITADO</strong> — ${res.accept_count} aceites, precisava ${res.quorum}`;
    } else {
      badge.textContent = '';
      panel.innerHTML = '<div style="color:var(--muted);font-size:13px;text-align:center;padding:20px 0">Aguardando próxima rodada de votação...</div>';
    }
  } catch(e) {}
}

function buildVoteCards(votes, finalResult) {
  if (!votes || Object.keys(votes).length === 0) {
    return '<div style="color:var(--muted);font-size:13px;text-align:center;padding:20px 0">Nenhum voto ainda</div>';
  }
  return `<div style="display:flex;gap:10px;flex-wrap:wrap;margin-top:4px">` +
    Object.entries(votes).map(([bank, state]) => {
      const c = VOTE_COLORS[state] || VOTE_COLORS.pending;
      const isLeader = finalResult && bank === finalResult.leader;
      return `<div style="
        background:${c.bg};border:2px solid ${c.border};
        border-radius:10px;padding:10px 14px;min-width:130px;text-align:center;
        transition:all 0.3s ease">
        <div style="font-size:22px;margin-bottom:4px">${c.icon}</div>
        <div style="font-weight:700;color:var(--accent2)">${bank}</div>
        ${isLeader ? '<div style="font-size:10px;color:var(--yellow)">★ Líder/Propositor</div>' : ''}
        <div style="font-size:12px;margin-top:4px;font-weight:600;color:${c.border}">${c.label}</div>
      </div>`;
    }).join('') + '</div>';
}

// ---- Manager vote popup (human-in-the-loop) ----
let voteModalBlock = null;   // block_index currently shown in modal
let voteDeadline = null;     // epoch ms when auto-fallback happens
let voteCountdownTimer = null;

async function loadPendingVote() {
  try {
    const r = await fetch('/api/pending-vote');
    const d = await r.json();
    const modal = $('vote-modal');

    if (!d.pending) {
      if (voteModalBlock !== null) closeVoteModal();
      return;
    }

    const c = d.candidate;
    // Only (re)initialize the modal when a NEW block arrives
    if (voteModalBlock !== c.block_index) {
      voteModalBlock = c.block_index;
      $('vm-index').textContent = c.block_index;
      $('vm-producer').textContent = c.producer_id;
      $('vm-orders').textContent = c.orders_count;
      $('vm-trades').textContent = c.trades_count;
      $('vm-hash').textContent = c.block_hash.slice(0, 24) + '...';

      // trade preview
      const tl = $('vm-tradelist');
      if (c.trades && c.trades.length) {
        tl.innerHTML = '<div style="font-size:11px;color:var(--muted);margin-bottom:5px">Negócios que serão registrados:</div>' +
          c.trades.slice(0, 6).map(t =>
            `<div class="trade-mini"><span><strong>${t.stock}</strong> ${t.quantity} un.</span><span>R$ ${Number(t.price).toFixed(2)}</span></div>`
          ).join('');
      } else {
        tl.innerHTML = '<div style="font-size:12px;color:var(--muted)">Nenhum trade neste bloco (sem casamento de ordens).</div>';
      }

      // auto recommendation
      const rec = $('vm-rec');
      rec.className = c.auto_recommendation ? 'rec-accept' : 'rec-reject';
      rec.innerHTML = c.auto_recommendation
        ? '🔍 Verificação automática: bloco <strong>VÁLIDO</strong> (hash, assinatura e leilão conferem) — recomenda APROVAR'
        : '🔍 Verificação automática: bloco <strong>INVÁLIDO</strong> — recomenda REJEITAR';

      // deadline countdown
      const received = new Date(c.received_at).getTime();
      voteDeadline = received + c.deadline_seconds * 1000;
      startVoteCountdown();

      modal.classList.add('show');
      showToast(`🗳️ Bloco #${c.block_index} aguardando sua votação!`, 'info');
    }
  } catch(e) {}
}

function startVoteCountdown() {
  if (voteCountdownTimer) clearInterval(voteCountdownTimer);
  const tick = () => {
    const rem = Math.max(0, Math.round((voteDeadline - Date.now()) / 1000));
    const el = $('vote-modal-timer');
    if (el) {
      el.textContent = rem;
      el.style.color = rem < 15 ? 'var(--red)' : rem < 30 ? 'var(--orange)' : 'var(--yellow)';
    }
    if (rem <= 0 && voteCountdownTimer) { clearInterval(voteCountdownTimer); voteCountdownTimer = null; }
  };
  tick();
  voteCountdownTimer = setInterval(tick, 1000);
}

function closeVoteModal() {
  voteModalBlock = null;
  voteDeadline = null;
  if (voteCountdownTimer) { clearInterval(voteCountdownTimer); voteCountdownTimer = null; }
  $('vote-modal').classList.remove('show');
}

async function castVote(approve) {
  try {
    const r = await fetch('/api/cast-vote', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({approve}),
    });
    if (r.ok) {
      showToast(approve ? '✅ Você APROVOU o bloco — voto assinado e enviado ao líder' : '❌ Você REJEITOU o bloco', approve ? 'ok' : 'info');
      closeVoteModal();
      setTimeout(refresh, 1500);
    } else {
      const d = await r.json();
      showToast('⚠ ' + (d.detail || 'erro ao votar'), 'info');
      closeVoteModal();
    }
  } catch(e) {
    showToast('Erro de conexão ao votar.', 'info');
  }
}

async function refresh() {
  await Promise.all([loadStatus(), loadPending(), loadBlocks(), loadTrades(), loadAuction(), loadVotes()]);
}

// Poll the pending-vote endpoint frequently so the popup appears promptly
setInterval(loadPendingVote, 1200);
loadPendingVote();

refresh();
setInterval(refresh, 2000);  // poll every 2s for responsive vote display
</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Client dashboard (separate, simplified — no consensus internals)
# ---------------------------------------------------------------------------

_CLIENT_HTML = """<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Home Broker — Cliente</title>
  <style>
    *{box-sizing:border-box;margin:0;padding:0}
    :root{
      --bg:#0b1020;--surface:#141a2e;--surface2:#1e2540;
      --accent:#4dabf7;--accent2:#74c0fc;--green:#51cf66;
      --red:#ff6b6b;--yellow:#ffd43b;--text:#e9ecef;--muted:#7d8799;--border:#28304e;
    }
    body{background:var(--bg);color:var(--text);font-family:'Segoe UI',system-ui,sans-serif;font-size:14px;line-height:1.5}
    header{background:linear-gradient(90deg,#141a2e,#1a2340);border-bottom:1px solid var(--border);padding:14px 24px;display:flex;align-items:center;gap:16px}
    header h1{font-size:19px;font-weight:700;color:var(--accent2)}
    .badge{display:inline-block;padding:2px 10px;border-radius:12px;font-size:12px;font-weight:600;background:#12233f;color:var(--accent2)}
    .navlink{margin-left:auto;color:var(--muted);text-decoration:none;font-size:13px;padding:4px 10px;border-radius:6px;border:1px solid var(--border)}
    .navlink:hover{color:var(--text);border-color:var(--accent)}
    main{padding:22px 24px;max-width:1200px;margin:0 auto}
    .grid-3{display:grid;grid-template-columns:repeat(3,1fr);gap:14px;margin-bottom:20px}
    .grid-2{display:grid;grid-template-columns:1.2fr 1fr;gap:20px}
    .card{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:18px}
    .card-label{font-size:11px;text-transform:uppercase;letter-spacing:.06em;color:var(--muted);margin-bottom:6px}
    .card-value{font-size:26px;font-weight:700;color:var(--accent2)}
    .card-sub{font-size:12px;color:var(--muted);margin-top:4px}
    .section{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:20px;margin-bottom:20px}
    .section h2{font-size:15px;font-weight:600;margin-bottom:16px}
    table{width:100%;border-collapse:collapse}
    th{text-align:left;padding:8px 10px;font-size:11px;text-transform:uppercase;color:var(--muted);border-bottom:1px solid var(--border)}
    td{padding:8px 10px;border-bottom:1px solid var(--border);font-size:13px}
    tr:last-child td{border-bottom:none}
    .form-row{display:flex;gap:12px;flex-wrap:wrap;align-items:flex-end}
    .form-group{display:flex;flex-direction:column;gap:5px}
    .form-group label{font-size:12px;color:var(--muted)}
    input,select{background:var(--surface2);border:1px solid var(--border);color:var(--text);padding:9px 11px;border-radius:7px;font-size:14px;outline:none}
    input:focus,select:focus{border-color:var(--accent)}
    select option{background:var(--surface2)}
    .btn{padding:10px 22px;border-radius:8px;border:none;cursor:pointer;font-size:14px;font-weight:700;transition:.15s}
    .btn-buy{background:var(--green);color:#04210d}
    .btn-buy:hover{filter:brightness(1.1)}
    .btn-sell{background:var(--red);color:#2a0808}
    .btn-sell:hover{filter:brightness(1.1)}
    .side-buy{color:var(--green);font-weight:700}
    .side-sell{color:var(--red);font-weight:700}
    .pill{padding:2px 9px;border-radius:10px;font-size:11px;font-weight:600}
    .pill-pending{background:#3a3010;color:var(--yellow)}
    .pill-done{background:#1a3a22;color:var(--green)}
    .pill-partial{background:#1a2a4a;color:var(--accent2)}
    .pill-exp{background:#2a2a2a;color:var(--muted)}
    .toast{position:fixed;top:20px;right:20px;padding:12px 20px;border-radius:10px;font-size:13px;font-weight:600;z-index:9999;opacity:0;transform:translateX(40px);transition:.3s;pointer-events:none;background:#12233f;color:var(--accent2);border:1px solid #24406a}
    .toast.show{opacity:1;transform:translateX(0)}
    .toast.ok{background:#1a3a22;color:var(--green);border-color:#2a5a32}
    .toast.err{background:#3a1a1a;color:var(--red);border-color:#5a2a2a}
    .idbox{display:flex;gap:10px;align-items:center}
    .countdown{font-size:22px;font-weight:800;color:var(--yellow);font-variant-numeric:tabular-nums}
    ::-webkit-scrollbar{width:6px;height:6px}
    ::-webkit-scrollbar-thumb{background:var(--border);border-radius:3px}
  </style>
</head>
<body>
<div id="toast" class="toast"></div>

<header>
  <h1>💹 Home Broker</h1>
  <span class="badge" id="hdr-bank">Banco —</span>
  <div class="idbox" style="margin-left:24px">
    <label style="font-size:12px;color:var(--muted)">Seu ID de investidor:</label>
    <input id="investor-id" placeholder="ex: Gustavo" style="width:160px"/>
    <button class="btn" style="background:var(--accent);color:#04182e;padding:8px 16px" onclick="saveInvestor()">Entrar</button>
  </div>
  <a href="/gestor" class="navlink" target="_blank">🏦 Painel do Gestor</a>
</header>

<main>
  <div class="grid-3">
    <div class="card">
      <div class="card-label">Saldo em Conta</div>
      <div class="card-value" id="c-cash">R$ —</div>
      <div class="card-sub" id="c-reserved">reservado: —</div>
    </div>
    <div class="card">
      <div class="card-label">Minhas Ordens Pendentes</div>
      <div class="card-value" id="c-myorders" style="color:var(--yellow)">—</div>
      <div class="card-sub">aguardando o próximo leilão</div>
    </div>
    <div class="card">
      <div class="card-label">Próximo Leilão</div>
      <div class="card-value countdown" id="c-countdown">—:——</div>
      <div class="card-sub">quando suas ordens serão processadas</div>
    </div>
  </div>

  <div class="section">
    <h2>Enviar Ordem</h2>
    <form id="order-form">
      <div class="form-row">
        <div class="form-group">
          <label>Ativo</label>
          <select id="f-stock">
            <option>PETR4</option><option>VALE3</option><option>ITUB4</option>
            <option>BBDC4</option><option>ABEV3</option><option>WEGE3</option>
            <option>RENT3</option><option>BBAS3</option><option>SUZB3</option>
            <option>RDOR3</option><option>RADL3</option><option>EGIE3</option>
            <option>LREN3</option><option>HAPV3</option><option>MGLU3</option>
          </select>
        </div>
        <div class="form-group">
          <label>Quantidade</label>
          <input type="number" id="f-qty" min="1" value="100" style="width:110px"/>
        </div>
        <div class="form-group">
          <label>Preço Limite (R$)</label>
          <input type="number" id="f-price" min="0.01" step="0.01" value="35.00" style="width:130px"/>
        </div>
        <div class="form-group">
          <label>&nbsp;</label>
          <button type="button" class="btn btn-buy" onclick="sendOrder('buy')">▲ Comprar</button>
        </div>
        <div class="form-group">
          <label>&nbsp;</label>
          <button type="button" class="btn btn-sell" onclick="sendOrder('sell')">▼ Vender</button>
        </div>
      </div>
      <div style="font-size:12px;color:var(--muted);margin-top:10px">
        Sua ordem é propagada para todos os bancos e entra no próximo leilão. Ordens de compra e venda com preços compatíveis são casadas.
      </div>
    </form>
  </div>

  <div class="grid-2">
    <div class="section">
      <h2>Minhas Ordens</h2>
      <div style="overflow-x:auto;max-height:320px;overflow-y:auto">
        <table>
          <thead><tr><th>Ativo</th><th>Lado</th><th>Qtd</th><th>Preço</th><th>Status</th></tr></thead>
          <tbody id="myorders-body"><tr><td colspan="5" style="color:var(--muted);text-align:center">Informe seu ID acima.</td></tr></tbody>
        </table>
      </div>
    </div>
    <div class="section">
      <h2>Minha Carteira</h2>
      <div style="overflow-x:auto;max-height:320px;overflow-y:auto">
        <table>
          <thead><tr><th>Ativo</th><th>Quantidade</th></tr></thead>
          <tbody id="portfolio-body"><tr><td colspan="2" style="color:var(--muted);text-align:center">—</td></tr></tbody>
        </table>
      </div>
    </div>
  </div>

  <div class="section">
    <h2>Mercado — Últimos Negócios</h2>
    <div style="overflow-x:auto">
      <table>
        <thead><tr><th>Ativo</th><th>Qtd</th><th>Preço</th><th>Bloco</th><th>Horário</th></tr></thead>
        <tbody id="market-body"><tr><td colspan="5" style="color:var(--muted);text-align:center">Sem negócios ainda</td></tr></tbody>
      </table>
    </div>
  </div>
</main>

<script>
const $ = id => document.getElementById(id);
let investor = localStorage.getItem('investor_id') || '';

function toast(msg, kind='') {
  const t = $('toast'); t.textContent = msg; t.className = 'toast show ' + kind;
  setTimeout(() => t.className = 'toast ' + kind, 3200);
}
function fmt(ts){ return ts ? new Date(ts).toLocaleTimeString('pt-BR') : '—'; }
function money(v){ return 'R$ ' + Number(v).toLocaleString('pt-BR',{minimumFractionDigits:2,maximumFractionDigits:2}); }

function saveInvestor() {
  investor = $('investor-id').value.trim();
  localStorage.setItem('investor_id', investor);
  toast(investor ? 'Bem-vindo, ' + investor : 'ID limpo', 'ok');
  refresh();
}

async function sendOrder(side) {
  if (!investor) { toast('Informe seu ID de investidor primeiro.', 'err'); return; }
  const payload = {
    investor_id: investor,
    stock: $('f-stock').value,
    side,
    quantity: parseInt($('f-qty').value),
    limit_price: parseFloat($('f-price').value),
  };
  try {
    const r = await fetch('/api/orders', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});
    const d = await r.json();
    if (r.ok) {
      toast(`${side==='buy'?'Compra':'Venda'} enviada: ${payload.quantity} ${payload.stock} @ ${money(payload.limit_price)}`, 'ok');
      setTimeout(refresh, 500);
    } else {
      toast('Erro: ' + (d.detail || 'falha'), 'err');
    }
  } catch(e) { toast('Erro de conexão.', 'err'); }
}

function statusPill(s, filledQty, qty) {
  s = (s || 'pending').toLowerCase();
  if (s === 'partial') return `<span class="pill pill-partial">parcial: ${filledQty}/${qty}</span>`;
  if (s.includes('match') || s.includes('fill') || s.includes('exec') || s.includes('done')) return '<span class="pill pill-done">executada</span>';
  if (s.includes('expir') || s.includes('cancel')) return '<span class="pill pill-exp">expirada</span>';
  return '<span class="pill pill-pending">pendente</span>';
}

async function loadStatus() {
  try {
    const s = await (await fetch('/api/status')).json();
    $('hdr-bank').textContent = 'Banco: ' + s.bank_id;
  } catch(e) {}
}

async function loadAuction() {
  try {
    const a = await (await fetch('/api/auction-status')).json();
    const rem = a.remaining_seconds, m = Math.floor(rem/60), sec = Math.floor(rem%60);
    $('c-countdown').textContent = `${m}:${String(sec).padStart(2,'0')}`;
  } catch(e) {}
}

async function loadPortfolio() {
  if (!investor) return;
  try {
    const r = await fetch('/api/portfolio/' + encodeURIComponent(investor));
    if (r.status === 404) {
      $('c-cash').textContent = money(0);
      $('c-reserved').textContent = 'conta ainda sem movimento';
      $('portfolio-body').innerHTML = '<tr><td colspan="2" style="color:var(--muted);text-align:center">Sem posições</td></tr>';
      return;
    }
    const d = await r.json();
    $('c-cash').textContent = money(d.cash_balance);
    $('c-reserved').textContent = 'reservado: ' + money(d.cash_reserved);
    const shares = Object.entries(d.shares || {}).filter(([k,v]) => v > 0);
    $('portfolio-body').innerHTML = shares.length
      ? shares.map(([stk,q]) => `<tr><td><strong>${stk}</strong></td><td>${q}</td></tr>`).join('')
      : '<tr><td colspan="2" style="color:var(--muted);text-align:center">Sem posições</td></tr>';
  } catch(e) {}
}


async function loadMyOrders() {
  if (!investor) return;
  try {
    const [dbRes, pendRes] = await Promise.all([
      fetch('/api/orders?limit=100'),
      fetch('/api/pending-orders'),
    ]);
    const d = await dbRes.json();
    const p = await pendRes.json();

    // Ordens já resolvidas (matched/partial/expired/...), vindas do banco de dados
    const settled = (d.orders || []).filter(o => (o.investor_id||'') === investor);


    const settledIds = new Set(settled.map(o => o.order_id));
    const waiting = (p.orders || [])
      .filter(o => (o.investor_id||'') === investor && !settledIds.has(o.order_id))
      .map(o => ({ ...o, status: 'pending', filled_quantity: 0 }));

    const mine = [...waiting, ...settled];

    $('c-myorders').textContent = mine.filter(o => {
      const s = (o.status||'pending').toLowerCase();
      return !(s.includes('match')||s.includes('exec')||s.includes('expir')||s.includes('cancel')||s.includes('done')||s.includes('fill')||s.includes('partial'));
    }).length;
    $('myorders-body').innerHTML = mine.length
      ? mine.slice(0,40).map(o => `<tr>
          <td><strong>${o.stock}</strong></td>
          <td class="${o.side==='buy'?'side-buy':'side-sell'}">${o.side==='buy'?'COMPRA':'VENDA'}</td>
          <td>${o.quantity}</td>
          <td>${money(o.limit_price)}</td>
          <td>${statusPill(o.status, o.filled_quantity, o.quantity)}</td></tr>`).join('')
      : '<tr><td colspan="5" style="color:var(--muted);text-align:center">Nenhuma ordem sua ainda</td></tr>';
  } catch(e) {}
}

async function loadMarket() {
  try {
    const d = await (await fetch('/api/trades?limit=25')).json();
    $('market-body').innerHTML = (d.trades && d.trades.length)
      ? d.trades.map(t => `<tr>
          <td><strong>${t.stock}</strong></td><td>${t.quantity}</td>
          <td>${money(t.price)}</td><td>#${t.block_index}</td><td>${fmt(t.traded_at)}</td></tr>`).join('')
      : '<tr><td colspan="5" style="color:var(--muted);text-align:center">Sem negócios ainda</td></tr>';
  } catch(e) {}
}

async function refresh() {
  await Promise.all([loadStatus(), loadAuction(), loadPortfolio(), loadMyOrders(), loadMarket()]);
}

if (investor) $('investor-id').value = investor;
refresh();
setInterval(refresh, 3000);
</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Portal (landing page — choose interface)
# ---------------------------------------------------------------------------

_PORTAL_HTML = """<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Exchange — Portal</title>
  <style>
    *{box-sizing:border-box;margin:0;padding:0}
    body{background:#0b1020;color:#e9ecef;font-family:'Segoe UI',system-ui,sans-serif;min-height:100vh;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:30px}
    h1{font-size:30px;color:#74c0fc}
    .sub{color:#7d8799;font-size:15px;margin-top:-16px}
    .cards{display:flex;gap:24px;flex-wrap:wrap;justify-content:center}
    .portal-card{background:#141a2e;border:1px solid #28304e;border-radius:16px;padding:34px 40px;width:300px;text-decoration:none;color:inherit;transition:.2s;text-align:center}
    .portal-card:hover{border-color:#4dabf7;transform:translateY(-4px)}
    .portal-card .ico{font-size:52px;margin-bottom:14px}
    .portal-card h2{font-size:20px;margin-bottom:8px}
    .portal-card p{font-size:13px;color:#7d8799}
    .g{color:#51cf66}.b{color:#74c0fc}
    .foot{color:#556;font-size:12px}
  </style>
</head>
<body>
  <div style="text-align:center">
    <h1>🏛️ Exchange Distribuída</h1>
    <div class="sub">Selecione a interface de acesso</div>
  </div>
  <div class="cards">
    <a class="portal-card" href="/gestor">
      <div class="ico">🏦</div>
      <h2 class="b">Gestor do Banco</h2>
      <p>Controle de leilão, votação de blocos, consenso BFT e monitoramento da rede.</p>
    </a>
    <a class="portal-card" href="/cliente">
      <div class="ico">💹</div>
      <h2 class="g">Cliente / Investidor</h2>
      <p>Enviar ordens de compra e venda, acompanhar carteira e negócios do mercado.</p>
    </a>
  </div>
  <div class="foot" id="foot">nó: carregando...</div>
  <script>
    fetch('/api/status').then(r=>r.json()).then(d=>{
      document.getElementById('foot').textContent =
        `nó ${d.bank_id} · ${d.chain_length} blocos · ${d.connected_peers.length} peers · BFT quórum ${d.bft_quorum}/${d.bft_n}`;
    }).catch(()=>{});
  </script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
async def portal():
    return _PORTAL_HTML


@app.get("/gestor", response_class=HTMLResponse)
async def manager_dashboard():
    return _HTML


@app.get("/cliente", response_class=HTMLResponse)
async def client_dashboard():
    return _CLIENT_HTML
