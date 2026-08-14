.PHONY: install format lint type test verify doctor

install:
	python -m pip install -e '.[dev]'

format:
	ruff format src tests
	ruff check --fix src tests

lint:
	ruff format --check src tests
	ruff check src tests

type:
	mypy

test:
	pytest

verify: lint type test doctor

doctor:
	interexchange-grid doctor --config config/defaults.yaml
