"""
The PizzaOrderWorkflow: one workflow execution per pizza order.

Workflow code must be DETERMINISTIC. Temporal records every step in an event
history, and if a worker crashes, another worker replays that history through
this code to rebuild state. So inside a workflow:
  - no network/disk I/O (use activities),
  - no random(), no datetime.now() (use workflow.now(), workflow.random()),
  - no asyncio.sleep() from the outside world (workflow.sleep / asyncio.sleep
    are fine *inside* a workflow because the SDK turns them into durable timers).

The Python SDK runs workflows in a sandbox that catches many mistakes for you.
"""

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

# Imports of non-workflow modules go through this block so the sandbox doesn't
# re-import them on every workflow run. Our activities are only referenced by
# name/signature here; they actually execute on the worker, outside the sandbox.
with workflow.unsafe.imports_passed_through():
    from pizza import activities
    from pizza.shared import OrderEvent, OrderState, OrderStatus, PizzaOrder


# --- Retry policies -------------------------------------------------------
# Tweak these and restart the worker to see the effect. Backoff between
# attempts is: initial_interval * backoff_coefficient^(attempt-1), capped at
# maximum_interval.

# Most steps: retry quickly, give up after 5 attempts.
DEFAULT_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=10),
    maximum_attempts=5,
)

# Validation: a bad order won't fix itself. InvalidOrder is raised as
# non_retryable in the activity, but you can also list error *types* here.
VALIDATE_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    maximum_attempts=3,
    non_retryable_error_types=["InvalidOrder"],
)

# Compensation must eventually succeed, so retry forever (maximum_attempts=0
# means unlimited). Inject failures into refund_payment to watch this.
REFUND_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=0,
)


@workflow.defn
class PizzaOrderWorkflow:
    def __init__(self) -> None:
        # Workflow state lives in plain instance variables. It survives worker
        # crashes because it's rebuilt by replaying history, not stored.
        self._order_id = ""
        self._state = OrderState.RECEIVED
        self._events: list[OrderEvent] = []
        self._payment_id: str | None = None
        self._driver: str | None = None
        self._tip_cents = 0
        self._cancel_requested = False
        self._delivery_confirmed = False

    # --- Queries: read-only, synchronous peeks at workflow state ------------
    @workflow.query
    def get_status(self) -> OrderStatus:
        return OrderStatus(
            order_id=self._order_id,
            state=self._state,
            events=self._events,
            payment_id=self._payment_id,
            driver=self._driver,
            tip_cents=self._tip_cents,
        )

    # --- Signals: fire-and-forget messages that can change state ------------
    @workflow.signal
    def cancel_order(self) -> None:
        # We only *record* the request; the main loop decides what to do.
        # Signal handlers should be quick and must also be deterministic.
        if self._state in (OrderState.OUT_FOR_DELIVERY, OrderState.AWAITING_CONFIRMATION):
            self._log(self._state, "Cancel ignored: pizza already left the shop")
            return
        self._cancel_requested = True
        self._log(self._state, "Cancel requested by customer")

    @workflow.signal
    def confirm_delivery(self) -> None:
        self._delivery_confirmed = True

    @workflow.signal
    def add_tip(self, cents: int) -> None:
        self._tip_cents += cents
        self._log(self._state, f"Tip added: {cents} cents")

    # --- The workflow itself ---------------------------------------------
    @workflow.run
    async def run(self, order: PizzaOrder) -> OrderStatus:
        self._order_id = order.order_id
        self._log(OrderState.RECEIVED, f"{len(order.pizzas)} pizza(s) for {order.customer_name}")

        try:
            # Step 1: validate
            self._log(OrderState.VALIDATING)
            await workflow.execute_activity(
                activities.validate_order,
                order,
                start_to_close_timeout=timedelta(seconds=5),
                retry_policy=VALIDATE_RETRY,
            )
            if self._cancel_requested:
                return self._finish(OrderState.CANCELLED, "Cancelled before payment")

            # Step 2: charge. From here on, a failure means we owe a refund.
            self._log(OrderState.CHARGING)
            self._payment_id = await workflow.execute_activity(
                activities.charge_payment,
                order,
                start_to_close_timeout=timedelta(seconds=5),
                retry_policy=DEFAULT_RETRY,
            )
            self._log(OrderState.CHARGING, f"Charged, payment id {self._payment_id}")
            if self._cancel_requested:
                return await self._refund_and_finish(order, OrderState.CANCELLED, "Cancelled after payment")

            # Step 3: bake (long-running, heartbeating activity)
            self._log(OrderState.BAKING)
            await workflow.execute_activity(
                activities.bake_pizza,
                order,
                start_to_close_timeout=timedelta(seconds=60),
                # If no heartbeat arrives for 5s, the attempt is considered
                # dead and retried (even though start_to_close is 60s).
                heartbeat_timeout=timedelta(seconds=5),
                retry_policy=DEFAULT_RETRY,
            )
            if self._cancel_requested:
                return await self._refund_and_finish(order, OrderState.CANCELLED, "Cancelled after baking")

            # Step 4: quality check
            self._log(OrderState.QUALITY_CHECK)
            await workflow.execute_activity(
                activities.quality_check,
                order,
                start_to_close_timeout=timedelta(seconds=5),
                retry_policy=DEFAULT_RETRY,
            )
            if self._cancel_requested:
                return await self._refund_and_finish(order, OrderState.CANCELLED, "Cancelled after QC")

            # Step 5: deliver
            self._log(OrderState.OUT_FOR_DELIVERY)
            self._driver = await workflow.execute_activity(
                activities.deliver_pizza,
                order,
                start_to_close_timeout=timedelta(seconds=10),
                # schedule_to_close bounds the TOTAL time across all attempts,
                # including backoff. Combine with delay_seconds to see it fire.
                schedule_to_close_timeout=timedelta(seconds=60),
                retry_policy=DEFAULT_RETRY,
            )
            self._log(OrderState.OUT_FOR_DELIVERY, f"Delivered by {self._driver}")

        except ActivityError as err:
            # An activity failed for good: retries exhausted, a timeout, or a
            # non-retryable error. err.cause holds the underlying reason.
            reason = f"{type(err.cause).__name__}: {err.cause}"
            if self._payment_id:
                await self._refund(order)
            self._log(OrderState.FAILED, reason)
            # Raising ApplicationError marks the workflow execution as Failed
            # in the Temporal UI. (Queries still work on closed workflows.)
            raise ApplicationError(f"Order {order.order_id} failed: {reason}") from err

        # Step 6: wait for the customer to confirm, using a durable timer.
        # This wait survives worker restarts: stop the worker, wait, start it
        # again, and the timer still fires at the right time.
        self._log(OrderState.AWAITING_CONFIRMATION)
        try:
            await workflow.wait_condition(
                lambda: self._delivery_confirmed,
                timeout=timedelta(seconds=order.confirm_timeout_seconds),
            )
            note = "Customer confirmed delivery"
        except asyncio.TimeoutError:
            note = f"No confirmation after {order.confirm_timeout_seconds}s, auto-confirmed"

        return self._finish(OrderState.DELIVERED, note)

    # --- helpers ------------------------------------------------------------
    def _log(self, state: OrderState, note: str = "") -> None:
        self._state = state
        self._events.append(OrderEvent(at=workflow.now().isoformat(), state=state, note=note))

    def _finish(self, state: OrderState, note: str) -> OrderStatus:
        self._log(state, note)
        return self.get_status()

    async def _refund(self, order: PizzaOrder) -> None:
        self._log(OrderState.REFUNDING, f"Refunding {self._payment_id}")
        await workflow.execute_activity(
            activities.refund_payment,
            args=[order, self._payment_id],
            start_to_close_timeout=timedelta(seconds=5),
            retry_policy=REFUND_RETRY,
        )

    async def _refund_and_finish(self, order: PizzaOrder, state: OrderState, note: str) -> OrderStatus:
        await self._refund(order)
        return self._finish(state, note)
