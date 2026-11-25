"""
Data types shared by the API, the workflow and the activities.

Temporal serializes workflow/activity inputs and outputs (by default as JSON).
Plain dataclasses work out of the box with the Python SDK's default data
converter, so we use them for everything that crosses a Temporal boundary.
"""

from dataclasses import dataclass, field
from enum import Enum

# Workers poll a *task queue* by name. The client starts workflows on the same
# name. If these don't match, your workflow will sit in "Running" forever with
# nobody picking it up (a classic first-day Temporal mistake, try it!).
TASK_QUEUE = "pizza-orders"


@dataclass
class FailureSpec:
    """
    How a single mocked activity should misbehave. All fields are optional.

    fail_times     Fail the first N attempts, then succeed. Good for watching
                   the retry policy kick in and eventually recover.
    failure_rate   0.0 - 1.0 chance that any given attempt fails (random).
    delay_seconds  Sleep this long before doing the work. Set it larger than
                   the activity's start_to_close_timeout to cause a TIMEOUT
                   instead of an exception.
    non_retryable  When the activity fails, raise a non-retryable error.
                   Temporal will NOT retry it, and the workflow sees the
                   failure immediately (it then runs compensation).
    """

    fail_times: int = 0
    failure_rate: float = 0.0
    delay_seconds: float = 0.0
    non_retryable: bool = False


@dataclass
class PizzaOrder:
    order_id: str
    customer_name: str
    address: str
    pizzas: list[str]
    # Price in cents, to avoid float money.
    total_cents: int
    # Keyed by activity name: "validate_order", "charge_payment",
    # "bake_pizza", "quality_check", "deliver_pizza", "refund_payment".
    failures: dict[str, FailureSpec] = field(default_factory=dict)
    # How long the workflow waits for the customer to confirm delivery before
    # auto-confirming. Shows off durable timers.
    confirm_timeout_seconds: int = 60


class OrderState(str, Enum):
    RECEIVED = "RECEIVED"
    VALIDATING = "VALIDATING"
    CHARGING = "CHARGING"
    BAKING = "BAKING"
    QUALITY_CHECK = "QUALITY_CHECK"
    OUT_FOR_DELIVERY = "OUT_FOR_DELIVERY"
    AWAITING_CONFIRMATION = "AWAITING_CONFIRMATION"
    DELIVERED = "DELIVERED"
    REFUNDING = "REFUNDING"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"


@dataclass
class OrderEvent:
    at: str  # ISO timestamp, taken from workflow.now() so it's deterministic
    state: OrderState
    note: str = ""


@dataclass
class OrderStatus:
    """What the `get_status` query returns."""

    order_id: str
    state: OrderState
    events: list[OrderEvent]
    payment_id: str | None = None
    driver: str | None = None
    tip_cents: int = 0
