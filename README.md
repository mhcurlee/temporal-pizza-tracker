# Pizza Tracker: a Temporal learning project

A FastAPI app where every pizza order is a **Temporal workflow**. The steps
(validate, charge, bake, quality check, deliver) are **activities** that mock
remote calls, and you can make any of them fail on purpose to watch Temporal's
retries, timeouts and error handling do their thing.

```
 HTTP client ──► FastAPI (pizza/api.py) ──start/signal/query──► Temporal server ◄──poll── Worker (pizza/worker.py)
                                                                 (stores history)          runs workflow + activities
```

## Quick start

You need Python 3.11+ and the `temporal` CLI (`brew install temporal`), or Docker.

```bash
make install
```

Then open three terminal tabs:

```bash
make server      # Temporal dev server, UI at http://localhost:8233
```

```bash
make worker      # runs your workflow and activity code
```

```bash
make api         # FastAPI, Swagger UI at http://localhost:8000/docs
```

No temporal CLI? `docker compose up` instead of `make server`.

**Front-end:** open http://localhost:8000 for a test page. Pick a failure preset (or set
fail N / rate / delay / non-retry per activity), place an order, and watch it live: the
current step, each in-flight activity's attempt count, last error and backoff countdown,
and buttons for the cancel / tip / confirm signals. It's a single static file,
`pizza/static/index.html`, calling the same endpoints as the curl examples below.

Place an order:

```bash
curl -s -XPOST localhost:8000/orders -H 'content-type: application/json' -d '{"pizzas":["pepperoni"]}'
```

Track it (or click the `temporal_ui` link it returns):

```bash
curl -s localhost:8000/orders/<order_id>
```

## Temporal concepts, mapped to this code

| Concept | Where | What to notice |
|---|---|---|
| **Workflow** | `pizza/workflows.py` `PizzaOrderWorkflow` | Ordinary async Python that is *durable*: its progress is stored as an event history, so it survives worker crashes and restarts. Must be deterministic. |
| **Activity** | `pizza/activities.py` | Where side effects live (network, randomness, I/O). Retried automatically per its retry policy. Should be idempotent (see the idempotency key in `charge_payment`). |
| **Worker** | `pizza/worker.py` | A process that polls a **task queue** and runs your code. The server never runs your code. |
| **Task queue** | `TASK_QUEUE` in `pizza/shared.py` | The name that connects clients to workers. |
| **Client** | `pizza/api.py` | Starts workflows, sends signals, runs queries. |
| **Retry policy** | top of `workflows.py` | Backoff, max attempts, non-retryable error types. `REFUND_RETRY` retries forever. |
| **Timeouts** | each `execute_activity(...)` call | `start_to_close` (one attempt), `schedule_to_close` (all attempts total), `heartbeat_timeout` (liveness), plus `execution_timeout` on the whole workflow in `api.py`. |
| **Heartbeats** | `bake_pizza` | Liveness reporting, and progress that a retried attempt can resume from. |
| **Query** | `get_status` | Read-only view of workflow state. `GET /orders/{id}` |
| **Signal** | `cancel_order`, `confirm_delivery`, `add_tip` | Async messages into a running workflow. |
| **Durable timer** | `wait_condition(..., timeout=...)` at the end of the workflow | Waits for the confirm signal, auto-confirms after `confirm_timeout_seconds`. Survives restarts. |
| **Saga / compensation** | `_refund` | If anything fails after payment, the workflow refunds. |
| **Workflow id** | the order id | Only one running workflow per id, which gives you dedup for free. |

## Inducing failures

Every order accepts a `failures` map keyed by activity name:
`validate_order`, `charge_payment`, `bake_pizza`, `quality_check`,
`deliver_pizza`, `refund_payment`. Each value can set:

| Field | Effect |
|---|---|
| `fail_times: N` | Fail the first N attempts, then succeed (uses `activity.info().attempt`). |
| `failure_rate: 0.3` | Each attempt fails with 30% probability. |
| `delay_seconds: S` | Sleep S seconds first. Exceed the activity's timeout to cause a timeout. |
| `non_retryable: true` | When it fails, fail with a non-retryable error: no retries at all. |

Recipes to try (watch the worker logs and the Temporal UI's event history):

**1. Retry and recover.** Payment fails twice, succeeds on attempt 3. Note the
growing gap between attempts (1s, 2s backoff).

```bash
curl -s -XPOST localhost:8000/orders -H 'content-type: application/json' -d '{"failures":{"charge_payment":{"fail_times":2}}}'
```

**2. Retries exhausted.** `DEFAULT_RETRY` allows 5 attempts, so 10 failures
means the activity gives up, the workflow refunds and ends as Failed.

```bash
curl -s -XPOST localhost:8000/orders -H 'content-type: application/json' -d '{"failures":{"bake_pizza":{"fail_times":10}}}'
```

**3. Non-retryable error.** One attempt, immediate failure, refund.

```bash
curl -s -XPOST localhost:8000/orders -H 'content-type: application/json' -d '{"failures":{"deliver_pizza":{"fail_times":1,"non_retryable":true}}}'
```

**4. Business validation error.** An unknown pizza raises `InvalidOrder`,
which is non-retryable. No charge happens, so no refund.

```bash
curl -s -XPOST localhost:8000/orders -H 'content-type: application/json' -d '{"pizzas":["anchovy"]}'
```

**5. Start-to-close timeout.** `quality_check` has a 5s timeout; a 7s delay
times out every attempt.

```bash
curl -s -XPOST localhost:8000/orders -H 'content-type: application/json' -d '{"failures":{"quality_check":{"delay_seconds":7}}}'
```

**6. Schedule-to-close timeout.** `deliver_pizza` gets 10s per attempt but
only 60s total. 8s delays plus a 50% failure rate will often run out the clock.

```bash
curl -s -XPOST localhost:8000/orders -H 'content-type: application/json' -d '{"failures":{"deliver_pizza":{"delay_seconds":8,"failure_rate":0.5}}}'
```

**7. Flaky compensation.** Make delivery fail for good *and* the refund fail
3 times. The refund retries until it works (unlimited attempts).

```bash
curl -s -XPOST localhost:8000/orders -H 'content-type: application/json' -d '{"failures":{"deliver_pizza":{"non_retryable":true,"fail_times":1},"refund_payment":{"fail_times":3}}}'
```

## Signals

```bash
curl -s -XPOST localhost:8000/orders/<id>/cancel
```

```bash
curl -s -XPOST localhost:8000/orders/<id>/tip -H 'content-type: application/json' -d '{"cents":300}'
```

```bash
curl -s -XPOST localhost:8000/orders/<id>/confirm
```

Cancel takes effect after the current step finishes (and refunds if you've
been charged). Once the pizza is out for delivery, cancel is ignored.

## Experiments beyond error injection

These show the "durable" in durable execution:

- **Kill the worker mid-bake.** Order two or three pizzas, Ctrl-C the worker
  while it logs "Baking", wait ~5s, start it again. The heartbeat timeout
  notices the dead attempt and the retry *resumes* from the last heartbeated
  second.
- **Stop the worker during the confirm wait.** Order with
  `"confirm_timeout_seconds": 30`, stop the worker once it's
  `AWAITING_CONFIRMATION`, start it a minute later. The timer already fired on
  the server; the workflow finishes right away.
- **No worker at all.** Stop the worker and place an order. It sits in
  Running with a pending workflow task. Start the worker and it proceeds.
  (`GET /orders/{id}` will time out meanwhile, because queries need a worker.)
- **Break determinism.** While an order is waiting for confirmation, add a new
  `execute_activity` call earlier in `run()`, then restart the worker. Replay
  no longer matches history and you'll see a nondeterminism error in the UI.
  That's why changing running workflows needs [versioning](https://docs.temporal.io/develop/python/versioning).
- **Scale out.** Run `make worker` in two tabs and place a few orders. Activities spread across both.

## Tests

```bash
make test
```

`tests/test_workflow.py` starts an in-process Temporal server (using your
`temporal` CLI binary) and checks retry, non-retryable, validation and cancel
behavior. A good pattern to copy for your own workflows.

## Project layout

```
pizza/shared.py      dataclasses + task queue name
pizza/failures.py    failure injection (maybe_fail)
pizza/activities.py  mocked remote calls
pizza/workflows.py   PizzaOrderWorkflow, retry policies
pizza/worker.py      worker process
pizza/api.py         FastAPI client
tests/               workflow tests
```

## Notes and assumptions

- Failure injection is per order (in the request body) rather than global
  config, so you can run a good order and a broken one side by side.
- `make server` persists to `temporal.db`, so history survives restarts.
  Delete the file to start clean. The docker-compose version is in-memory,
  and I have not run it yet (I tested with the temporal CLI).
- Ideas for next steps: replace `add_tip` with a workflow **Update** (a
  signal that returns a value and can validate input), add a child workflow
  per pizza, or use `continue_as_new` for a long-lived "store" workflow.
