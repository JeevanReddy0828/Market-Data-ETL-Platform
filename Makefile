.PHONY: lint test run

lint:
	python -m ruff check .

test:
	python -m pytest

run:
	python -m src.cli run-once --date 2024-01-02
