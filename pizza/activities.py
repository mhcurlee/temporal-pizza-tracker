"""
Activities: the steps that talk to the outside world.

In Temporal, anything non-deterministic (network calls, randomness, reading
the clock, disk I/O) belongs in an activity, never in workflow code. If an
activity fails, Temporal retries it according to its RetryPolicy; the
workflow just sees the final result or the final failure.

Every "remote call" here is mocked with asyncio.sleep. Each activity calls
`maybe_fail` first so you can inject errors per order (see failures.py).
"""

import asyncio
import random
import uuid

from temporalio import activity
from temporalio.exceptions import ApplicationError

from pizza.failures import maybe_fail
from pizza.shared import PizzaOrder

MENU_PRICES_CENTS = {
    "margherita": 1200,
    "pepperoni": 1400,
    "hawaiian": 1400,
    "veggie": 1300,
    "meat-lovers": 1600,
}

DRIVERS = ["Ada", "Linus", "Grace", "Guido", "Barbara"]


@activity.defn
async def validate_order(order: PizzaOrder) -> None:
    """Pretend to call a menu/inventory service."""
    activity.logger.info("Validating order %s", order.order_id)
    await maybe_fail("validate_order", order)
    await asyncio.sleep(0.5)

    # Real business validation. A bad order will never become valid by
    # retrying, so these are non-retryable errors.
    if not order.pizzas:
        raise ApplicationError("Order has no pizzas", type="InvalidOrder", non_retryable=True)
    unknown = [p for p in order.pizzas if p not in MENU_PRICES_CENTS]
    if unknown:
        raise ApplicationError(
            f"Unknown pizzas: {unknown}. Menu: {sorted(MENU_PRICES_CENTS)}",
            type="InvalidOrder",
            non_retryable=True,
        )


@activity.defn
async def charge_payment(order: PizzaOrder) -> str:
    """Pretend to call a payment provider. Returns a payment id.

    Real payment APIs take an idempotency key so a retried request doesn't
    charge the customer twice. Temporal gives at-least-once activity
    execution, so idempotency is YOUR job. The workflow id (order id) plus
    activity id is a natural key.
    """
    info = activity.info()
    idempotency_key = f"{info.workflow_id}-{info.activity_id}"
    activity.logger.info(
        "Charging %d cents for %s (idempotency key %s, attempt %d)",
        order.total_cents,
        order.order_id,
        idempotency_key,
        info.attempt,
    )
    await maybe_fail("charge_payment", order)
    await asyncio.sleep(1)
    return f"pay_{uuid.uuid4().hex[:10]}"


@activity.defn
async def bake_pizza(order: PizzaOrder) -> None:
    """A long-running activity that heartbeats.

    Heartbeats do two things:
      1. Liveness. If the worker dies mid-bake, Temporal notices after
         heartbeat_timeout instead of waiting for the full start_to_close.
      2. Progress. The details you pass to heartbeat() are handed back to the
         next attempt via info.heartbeat_details, so a retry can resume
         instead of starting over. Kill the worker mid-bake and watch it
         resume from the last second it reported!
    """
    info = activity.info()
    total_seconds = 2 * len(order.pizzas) + 2
    start_from = info.heartbeat_details[0] if info.heartbeat_details else 0
    if start_from:
        activity.logger.info("Resuming bake at %ds (from heartbeat)", start_from)

    await maybe_fail("bake_pizza", order)

    for second in range(start_from, total_seconds):
        activity.logger.info("Baking %s: %d/%ds", order.order_id, second + 1, total_seconds)
        await asyncio.sleep(1)
        activity.heartbeat(second + 1)


@activity.defn
async def quality_check(order: PizzaOrder) -> None:
    """Pretend a chef inspects the pizzas."""
    activity.logger.info("Quality checking %s", order.order_id)
    await maybe_fail("quality_check", order)
    await asyncio.sleep(0.5)


@activity.defn
async def deliver_pizza(order: PizzaOrder) -> str:
    """Pretend to dispatch a driver. Returns the driver name."""
    driver = random.choice(DRIVERS)
    activity.logger.info("Driver %s delivering %s to %s", driver, order.order_id, order.address)
    await maybe_fail("deliver_pizza", order)
    await asyncio.sleep(3)
    return driver


@activity.defn
async def refund_payment(order: PizzaOrder, payment_id: str) -> None:
    """Compensation: undo charge_payment when a later step fails for good."""
    activity.logger.info("Refunding payment %s for %s", payment_id, order.order_id)
    await maybe_fail("refund_payment", order)
    await asyncio.sleep(0.5)


ALL_ACTIVITIES = [
    validate_order,
    charge_payment,
    bake_pizza,
    quality_check,
    deliver_pizza,
    refund_payment,
]
