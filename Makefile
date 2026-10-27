.PHONY: help setup data pipeline app test practice lint format docker-test docker-practice iac

STAGE ?= bronze

help:  ## Show available targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-16s %s\n", $$1, $$2}'

setup:  ## Install everything (Python 3.12, PySpark, Delta, app, dev tools)
	uv sync --all-extras

data:  ## Regenerate the synthetic landing zone in data/raw (deterministic)
	uv run utilization generate --out data/raw

pipeline:  ## Run bronze -> silver -> gold -> quality into ./lakehouse
	uv run utilization run --stage all --landing data/raw --lakehouse lakehouse

app:  ## Serve the dashboard on http://localhost:8000 (needs ./lakehouse/gold)
	uv run uvicorn dashboard.main:app --app-dir app --reload --port 8000

test:  ## Reference solution: full suite
	uv run pytest

practice:  ## Your attempt: make STAGE=bronze|silver|gold practice
	uv run pytest --impl starter -k $(STAGE)

lint:  ## Ruff lint + format check
	uv run ruff check .
	uv run ruff format --check .

format:  ## Auto-fix lint and formatting
	uv run ruff check --fix .
	uv run ruff format .

docker-test:  ## Full suite inside the dev container (Windows: Spark needs it)
	docker compose run --rm dev pytest

docker-practice:  ## Your attempt inside the dev container: make STAGE=silver docker-practice
	docker compose run --rm dev pytest --impl starter -k $(STAGE)

iac:  ## terraform fmt + validate (in a container, no install needed)
	docker run --rm -v "$(CURDIR)/infra/terraform:/tf" -w /tf --entrypoint sh hashicorp/terraform:1.9 \
		-c "terraform fmt -check && terraform init -backend=false -input=false >/dev/null && terraform validate; rm -rf .terraform"
