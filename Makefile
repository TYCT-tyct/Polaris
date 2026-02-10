.PHONY: install test test-live lint migrate harvest-once run

install:
	python -m pip install -e .[dev]

test:
	pytest

test-live:
	pytest -m live

lint:
	ruff check polaris tests

migrate:
	python -m polaris.cli migrate

harvest-once:
	python -m polaris.cli harvest-once --handle elonmusk

run:
	python -m polaris.cli run --handle elonmusk

