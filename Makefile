.PHONY: setup lint type test dbcheck
setup:
	python -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt -r requirements-dev.txt
lint:
	. .venv/bin/activate && ruff check .
type:
	. .venv/bin/activate && mypy src
test:
	. .venv/bin/activate && pytest -q
dbcheck:
	. .venv/bin/activate && python scripts/db_smoke_check.py
.PHONY: fetch-db
fetch-db:
	@TAG?=
ifdef TAG
	@echo "Using TAG=$(TAG)"
	TAG=$(TAG) scripts/fetch_public_db.sh
else
	scripts/fetch_public_db.sh
endif
