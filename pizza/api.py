"""
FastAPI front end. It never runs pizza logic itself; it is a Temporal *client*
that starts workflows, sends them signals and queries their state.

Run with:  uvicorn pizza.api:app --reload
Then open  http://localhost:8000/docs  for an interactive Swagger UI.
"""

import os
import uuid
from contextlib import asynccontextmanager
from datetime import timedelta

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field
from temporalio.client import Client, WorkflowHandle
from temporalio.service import RPCError, RPCStatusCode

from pizza.activities import MENU_PRICES_CENTS
from pizza.shared import TASK_QUEUE, FailureSpec, OrderStatus, PizzaOrder
from pizza.workflows import PizzaOrderWorkflow

TEMPORAL_ADDRESS = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
TEMPORAL_UI = os.environ.get("TEMPORAL_UI", "http://localhost:8233")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # One client for the whole app. It's cheap to share and thread safe.
    app.state.temporal = await Client.connect(TEMPORAL_ADDRESS)
    yield


app = FastAPI(title="Pizza Tracker (Temporal demo)", lifespan=lifespan)


# --- Request/response models -------------------------------------------------
# Pydantic models for the HTTP layer; we convert them to the dataclasses in
# shared.py before handing them to Temporal.


class FailureSpecIn(BaseModel):
    fail_times: int = Field(0, ge=0, description="Fail the first N attempts, then succeed")
    failure_rate: float = Field(0.0, ge=0, le=1, description="Random failure probability per attempt")
    delay_seconds: float = Field(0.0, ge=0, description="Sleep before working (use to trigger timeouts)")
    non_retryable: bool = Field(False, description="Failures are non-retryable (no retries at all)")


class OrderIn(BaseModel):
    customer_name: str = "Marvin"
    address: str = "42 Durable Execution Way"
    pizzas: list[str] = Field(default_factory=lambda: ["margherita"])
    confirm_timeout_seconds: int = Field(60, ge=1)
    failures: dict[str, FailureSpecIn] = Field(
        default_factory=dict,
        description=(
            "Per-activity failure injection. Keys: validate_order, charge_payment, "
            "bake_pizza, quality_check, deliver_pizza, refund_payment"
        ),
        examples=[{"charge_payment": {"fail_times": 2}}],
    )


class OrderCreated(BaseModel):
    order_id: str
    temporal_ui: str


class TipIn(BaseModel):
    cents: int = Field(..., gt=0)


# --- Helpers -------------------------------------------------------------------


def _client(request: Request) -> Client:
    return request.app.state.temporal


async def _existing_handle(request: Request, order_id: str) -> WorkflowHandle:
    handle = _client(request).get_workflow_handle(order_id)
    try:
        await handle.describe()
    except RPCError as err:
        if err.status == RPCStatusCode.NOT_FOUND:
            raise HTTPException(404, f"No order {order_id}")
        raise
    return handle


# --- Endpoints -------------------------------------------------------------------


@app.get("/menu")
async def menu() -> dict[str, int]:
    return MENU_PRICES_CENTS


@app.post("/orders", status_code=201)
async def place_order(body: OrderIn, request: Request) -> OrderCreated:
    order_id = f"order-{uuid.uuid4().hex[:8]}"
    order = PizzaOrder(
        order_id=order_id,
        customer_name=body.customer_name,
        address=body.address,
        pizzas=body.pizzas,
        # Unknown pizzas price at 0 here; validate_order will reject them.
        total_cents=sum(MENU_PRICES_CENTS.get(p, 0) for p in body.pizzas),
        failures={name: FailureSpec(**spec.model_dump()) for name, spec in body.failures.items()},
        confirm_timeout_seconds=body.confirm_timeout_seconds,
    )
    # start_workflow returns as soon as the server has durably recorded the
    # start. The pizza is "being made" from here on even if this API dies.
    # The workflow id doubles as the order id, and Temporal guarantees only one
    # running workflow per id, which gives us free deduplication.
    await _client(request).start_workflow(
        PizzaOrderWorkflow.run,
        order,
        id=order_id,
        task_queue=TASK_QUEUE,
        # Safety net: no order should take longer than this in total.
        execution_timeout=timedelta(minutes=30),
    )
    return OrderCreated(
        order_id=order_id,
        temporal_ui=f"{TEMPORAL_UI}/namespaces/default/workflows/{order_id}",
    )


@app.get("/orders")
async def list_orders(request: Request) -> list[dict]:
    """Uses Temporal's visibility API (a searchable index of executions)."""
    results = []
    # Visibility queries use a SQL-like syntax. (The dev server's SQLite
    # visibility store doesn't support ORDER BY; results come newest first.)
    query = "WorkflowType = 'PizzaOrderWorkflow'"
    async for wf in _client(request).list_workflows(query, limit=50):
        results.append(
            {
                "order_id": wf.id,
                "execution_status": wf.status.name if wf.status else None,
                "started": wf.start_time.isoformat(),
            }
        )
    return results


@app.get("/orders/{order_id}")
async def get_order(order_id: str, request: Request) -> dict:
    """Combines a workflow *query* (our app-level state) with *describe*
    (Temporal's view: RUNNING, COMPLETED, FAILED, ...)."""
    handle = await _existing_handle(request, order_id)
    desc = await handle.describe()
    # Queries are answered by a worker, so this needs the worker running.
    status: OrderStatus = await handle.query(
        PizzaOrderWorkflow.get_status, rpc_timeout=timedelta(seconds=5)
    )
    return {"execution_status": desc.status.name if desc.status else None, "order": status}


@app.post("/orders/{order_id}/cancel", status_code=202)
async def cancel_order(order_id: str, request: Request) -> dict:
    handle = await _existing_handle(request, order_id)
    await handle.signal(PizzaOrderWorkflow.cancel_order)
    return {"signal": "cancel_order", "sent": True}


@app.post("/orders/{order_id}/confirm", status_code=202)
async def confirm_delivery(order_id: str, request: Request) -> dict:
    handle = await _existing_handle(request, order_id)
    await handle.signal(PizzaOrderWorkflow.confirm_delivery)
    return {"signal": "confirm_delivery", "sent": True}


@app.post("/orders/{order_id}/tip", status_code=202)
async def add_tip(order_id: str, body: TipIn, request: Request) -> dict:
    handle = await _existing_handle(request, order_id)
    await handle.signal(PizzaOrderWorkflow.add_tip, body.cents)
    return {"signal": "add_tip", "cents": body.cents}
