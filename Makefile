PYTHON ?= python
PACKAGE = src tests

.PHONY: help format lint test check clean

help:
	@echo "Available targets:"
	@echo "  make format   - Run black and ruff auto-fixes"
	@echo "  make lint     - Run ruff and black checks"
	@echo "  make test     - Run pytest"
	@echo "  make check    - Run lint and tests"
	@echo "  make clean    - Remove caches"

format:
	$(PYTHON) -m black $(PACKAGE)
	$(PYTHON) -m ruff check --fix $(PACKAGE)

lint:
	$(PYTHON) -m ruff check $(PACKAGE)
	$(PYTHON) -m black --check $(PACKAGE)

test:
	$(PYTHON) -m pytest

check:
	$(PYTHON) -m ruff check $(PACKAGE)
	$(PYTHON) -m black --check $(PACKAGE)
	$(PYTHON) -m pytest

clean:
	find . -type d -name "__pycache__" -prune -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -prune -exec rm -rf {} +
	find . -type d -name ".ruff_cache" -prune -exec rm -rf {} +
	find . -type d -name ".mypy_cache" -prune -exec rm -rf {} +