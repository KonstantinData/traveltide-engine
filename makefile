install:
	pip install poetry
	poetry install

test:
	poetry run pytest tests/

lint:
	poetry run ruff check src/

run:
	poetry run python main.py run-pipeline

clean:
	rm -rf __pycache__ .pytest_cache