.PHONY: clean dev ci test integration fmt help docs-install docs-clean docs-build docs-serve

clean:
	rm -rf .venv .pytest_cache .ruff_cache .mypy_cache __pycache__
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true

dev:
	uv sync

ci:
	uv sync --frozen

test:
	PYTHONPATH=src uv run pytest tests/unit -v

integration:
	PYTHONPATH=src uv run pytest tests/integration -v -m "not slow"

fmt:
	uv run ruff format src/ tests/
	uv run ruff check src/ tests/ --fix
	uv run mypy src/flowx/

docs-install:
	cd docs && bun install --frozen-lockfile

docs-clean:
	rm -rf docs/.next docs/.source docs/out docs/site docs/node_modules

docs-build: docs-install
	cd docs && bun run build && rm -rf site && mv out site

docs-serve: docs-build
	cd docs && bun run dev

help:
	@echo "Available targets:"
	@echo "  dev          Install dependencies"
	@echo "  ci           Install dependencies (frozen lockfile)"
	@echo "  test         Run unit tests"
	@echo "  integration  Run integration tests"
	@echo "  fmt          Format and lint code"
	@echo "  clean        Remove build artifacts"
	@echo "  docs-install Install docs dependencies (bun)"
	@echo "  docs-clean   Remove docs build artifacts"
	@echo "  docs-build   Build the static docs site to docs/site"
	@echo "  docs-serve   Run the docs dev server (next dev)"
