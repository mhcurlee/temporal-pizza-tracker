"""
Failure injection for the mocked activities.

Every activity calls `maybe_fail(name, order)` before doing its "remote call".
The decision is driven by the FailureSpec the client sent with the order.

The trick behind `fail_times` is `activity.info().attempt`: Temporal tells
each activity execution which attempt it is (1, 2, 3, ...). So "fail the first
2 attempts" is just `attempt <= 2`. No shared state needed, which matters
because retries may land on a different worker process.
"""

import asyncio
import random

from temporalio import activity
from temporalio.exceptions import ApplicationError

from pizza.shared import PizzaOrder


class MockRemoteServiceError(Exception):
    """A plain (retryable) error, like a 503 from a flaky upstream."""


async def maybe_fail(activity_name: str, order: PizzaOrder) -> None:
    spec = order.failures.get(activity_name)
    if spec is None:
        return

    info = activity.info()
    attempt = info.attempt

    # 1. Slow call. If this exceeds start_to_close_timeout, Temporal times the
    #    attempt out and retries it. We sleep in small steps and heartbeat so
    #    heartbeat-enabled activities keep reporting liveness while "slow".
    if spec.delay_seconds > 0:
        activity.logger.info(
            "[%s] attempt %d: injected delay of %.1fs",
            activity_name,
            attempt,
            spec.delay_seconds,
        )
        remaining = spec.delay_seconds
        while remaining > 0:
            step = min(1.0, remaining)
            await asyncio.sleep(step)
            remaining -= step
            activity.heartbeat(f"delaying, {remaining:.1f}s left")

    should_fail = attempt <= spec.fail_times or (
        spec.failure_rate > 0 and random.random() < spec.failure_rate
    )
    if not should_fail:
        return

    msg = f"[{activity_name}] injected failure on attempt {attempt}"
    activity.logger.warning(msg)

    if spec.non_retryable:
        # ApplicationError(non_retryable=True) tells Temporal "don't bother
        # retrying, this will never succeed" (e.g. card declined).
        raise ApplicationError(msg, type="InjectedNonRetryable", non_retryable=True)

    # Any other exception is retryable by default, per the retry policy.
    raise MockRemoteServiceError(msg)
