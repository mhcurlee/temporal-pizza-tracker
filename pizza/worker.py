"""
The worker: a long-running process that polls a task queue and executes
workflow and activity code.

The Temporal *server* never runs your code. It only stores history and hands
out tasks. Workers (this file) do the actual work. Run several copies of it
and Temporal load-balances between them; kill one mid-order and another picks
up where it left off.

Run with:  python -m pizza.worker
"""

import asyncio
import logging
import os

from temporalio.client import Client
from temporalio.worker import Worker

from pizza.activities import ALL_ACTIVITIES
from pizza.shared import TASK_QUEUE
from pizza.workflows import PizzaOrderWorkflow

TEMPORAL_ADDRESS = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    client = await Client.connect(TEMPORAL_ADDRESS)
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[PizzaOrderWorkflow],
        activities=ALL_ACTIVITIES,
    )
    logging.info("Worker polling task queue %r on %s", TASK_QUEUE, TEMPORAL_ADDRESS)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
