.PHONY: clean dev ci test integration fmt help docs-install docs-clean docs-build docs-serve lock-dependencies requirements precommit

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
	uv run mypy src/orchestra/

docs-install:
	cd docs && bun install --frozen-lockfile

docs-clean:
	rm -rf docs/.next docs/.source docs/out docs/site docs/node_modules

docs-build: docs-install
	cd docs && bun run build && rm -rf site && mv out site

docs-serve: docs-build
	cd docs && bun run dev


lock-dependencies: export UV_FROZEN := 0
lock-dependencies:
	uv lock --exclude-newer "7 days"
	uv run --exact --all-extras --group yq tomlq -r '.["build-system"].requires[]' pyproject.toml | \
	  uv pip compile --generate-hashes --universal --no-header - > build-constraints-new.txt
	mv build-constraints-new.txt .build-constraints.txt
	perl -pi -e 's|registry = "https://[^"]*"|registry = "https://pypi.org/simple"|g' uv.lock
	$(MAKE) requirements

requirements:
	uv export --frozen --no-dev --no-emit-project --no-hashes --format requirements-txt -o requirements.txt

precommit: fmt requirements

help:
	@echo "Available targets:"
	@echo "  dev          		Install dependencies"
	@echo "  ci           		Install dependencies (frozen lockfile)"
	@echo "  test         		Run unit tests"
	@echo "  integration  		Run integration tests"
	@echo "  fmt          		Format and lint code"
	@echo "  precommit    		Format, lint, and refresh requirements.txt (run before committing)"
	@echo "  clean        		Remove build artifacts"
	@echo "  docs-install 		Install docs dependencies (bun)"
	@echo "  docs-clean   		Remove docs build artifacts"
	@echo "  docs-build   		Build the static docs site to docs/site"
	@echo "  docs-serve   		Run the docs dev server (next dev)"
	@echo "  lock-dependencies	Write the uv.lock file"
	@echo "  requirements 		Generate requirements.txt from the lockfile"
