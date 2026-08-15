.PHONY: install format lint type test verify lock doctor

install:
	python -m pip install -r requirements.lock
	python -m pip install -e . --no-deps --no-build-isolation

format:
	ruff format src tests scripts
	ruff check --fix src tests scripts

lint:
	ruff format --check src tests scripts
	ruff check src tests scripts

type:
	mypy

test:
	pytest

verify: lock lint type test doctor

lock:
	python scripts/check_lock.py --lock requirements.lock --pyproject pyproject.toml

doctor:
	interexchange-grid doctor --config config/defaults.yaml
