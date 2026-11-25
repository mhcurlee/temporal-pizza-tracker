"""
Workflow tests. Temporal ships a test environment that runs a real server
in-process, so tests exercise the actual retry machinery.

We point it at your installed `temporal` CLI (so nothing is downloaded).
Run with:  make test
"""

import shutil
import uuid

import pytest
from temporalio.client import WorkflowFailureError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from pizza.activities import ALL_ACTIVITIES
from pizza.shared import FailureSpec, OrderState, PizzaOrder
from pizza.workflows import PizzaOrderWorkflow

TQ = "test-pizza"


@pytest.fixture(scope="session")
async def env():
    async with await WorkflowEnvironment.start_local(
        dev_server_existing_path=shutil.which("temporal")
    ) as env:
        yield env


@pytest.fixture
async def worker(env):
    async with Worker(
        env.client, task_queue=TQ, workflows=[PizzaOrderWorkflow], activities=ALL_ACTIVITIES
    ):
        yield


def make_order(**failures: FailureSpec) -> PizzaOrder:
    return PizzaOrder(
        order_id=f"test-{uuid.uuid4().hex[:6]}",
        customer_name="Test",
        address="1 Test St",
        pizzas=["margherita"],
        total_cents=1200,
        failures=failures,
        confirm_timeout_seconds=1,
    )


async def test_retries_then_succeeds(env, worker):
    order = make_order(charge_payment=FailureSpec(fail_times=2))
    result = await env.client.execute_workflow(
        PizzaOrderWorkflow.run, order, id=order.order_id, task_queue=TQ
    )
    assert result.state == OrderState.DELIVERED
    assert result.payment_id

    # The event history proves Temporal retried: attempt 3 succeeded.
    handle = env.client.get_workflow_handle(order.order_id)
    attempts = [
        e.activity_task_started_event_attributes.attempt
        async for e in handle.fetch_history_events()
        if e.HasField("activity_task_started_event_attributes")
    ]
    assert 3 in attempts


async def test_non_retryable_fails_and_refunds(env, worker):
    order = make_order(bake_pizza=FailureSpec(fail_times=1, non_retryable=True))
    handle = await env.client.start_workflow(
        PizzaOrderWorkflow.run, order, id=order.order_id, task_queue=TQ
    )
    with pytest.raises(WorkflowFailureError):
        await handle.result()
    status = await handle.query(PizzaOrderWorkflow.get_status)
    assert status.state == OrderState.FAILED
    assert OrderState.REFUNDING in [e.state for e in status.events]


async def test_invalid_order_is_not_retried(env, worker):
    order = make_order()
    order.pizzas = ["anchovy"]
    with pytest.raises(WorkflowFailureError):
        await env.client.execute_workflow(
            PizzaOrderWorkflow.run, order, id=order.order_id, task_queue=TQ
        )


async def test_cancel_signal_refunds(env, worker):
    # Slow down baking so we have time to send the cancel signal.
    order = make_order(bake_pizza=FailureSpec(delay_seconds=2))
    handle = await env.client.start_workflow(
        PizzaOrderWorkflow.run, order, id=order.order_id, task_queue=TQ
    )
    await handle.signal(PizzaOrderWorkflow.cancel_order)
    result = await handle.result()
    assert result.state == OrderState.CANCELLED
