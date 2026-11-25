# Each target runs in the foreground; use three terminal tabs.
PY := .venv/bin/python

.PHONY: install server worker api test

install:            ## create venv and install deps
	python3 -m venv .venv && .venv/bin/pip install -r requirements.txt

server:             ## Temporal dev server + UI on http://localhost:8233 (persists to temporal.db)
	temporal server start-dev --db-filename temporal.db

worker:             ## run the worker (restart it after changing workflow/activity code)
	$(PY) -m pizza.worker

api:                ## FastAPI on http://localhost:8000/docs
	$(PY) -m uvicorn pizza.api:app --reload

test:               ## run tests (uses Temporal's in-process test server, no `make server` needed)
	$(PY) -m pytest -v
