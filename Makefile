.DEFAULT_GOAL := help

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install with dev dependencies
	pip install -e ".[dev]"

migrate: ## Apply the dq schema
	python -m meridian_pipelines migrate

check: ## Run data quality checks (exit 1 blocks the pipeline)
	python -m meridian_pipelines check --source-dir ../meridian-data-generator/output/parquet

reconcile: ## Compare source and warehouse totals
	python -m meridian_pipelines reconcile --source-dir ../meridian-data-generator/output/parquet

quarantine: ## Show unresolved quarantined rows
	python -m meridian_pipelines quarantine-report

test: ## Run the test suite
	pytest -q

lint: ## Ruff lint + format check (same as CI)
	ruff check src tests && ruff format --check src tests

fmt: ## Auto-fix lint and formatting
	ruff check --fix src tests && ruff format src tests
