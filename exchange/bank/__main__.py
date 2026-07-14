from __future__ import annotations

import asyncio
import logging
import os

import uvicorn

from .api import init_app
from .config import load_config
from .node import BankNode

logging.basicConfig(
    level=getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)


async def _main() -> None:
    config = load_config()
    node = BankNode(config.this_bank.bank_id, config)
    app = init_app(node)

    await node.start()

    server_cfg = uvicorn.Config(
        app,
        host="0.0.0.0",
        port=config.api_port,
        loop="asyncio",
        log_level="info",
        access_log=False,
    )
    server = uvicorn.Server(server_cfg)
    try:
        await server.serve()
    finally:
        await node.stop()


if __name__ == "__main__":
    asyncio.run(_main())
