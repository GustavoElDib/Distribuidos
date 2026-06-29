"""Entry point: python -m bank.node"""
from __future__ import annotations

import asyncio
import logging
import signal

from .config import load_config
from .node import BankNode

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)


async def _main() -> None:
    config = load_config()
    node = BankNode(config.this_bank.bank_id, config)

    loop = asyncio.get_running_loop()

    def _shutdown() -> None:
        asyncio.create_task(node.stop())

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown)

    await node.start()
    # keep running until stop() is called
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(_main())
