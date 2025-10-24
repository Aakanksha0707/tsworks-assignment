.PHONY: venv install init test

venv:
	python -m venv .venv

install:
	. .venv/bin/activate && pip install -r requirements.txt

init:
	. .venv/bin/activate && python scripts/init_db.py

test:
	. .venv/bin/activate && python -m pytest -q
