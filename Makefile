.PHONY: tests

build:
	uv pip install -e .
	uv pip install -e ./bafrapy_sqlacodegen --force-reinstall

serve:
	docker volume create bafrapy-postgresql-data >/dev/null
	docker volume create bafrapy-directus-data >/dev/null
	docker volume create bafrapy-ducklake-data >/dev/null
	docker compose down
	docker compose up -d

serve-down:
	docker compose down

phpmyadmin:
	docker compose -f docker-compose-phpmyadmin.yml down
	docker compose -f docker-compose-phpmyadmin.yml up -d

phpmyadmin-down:
	docker compose -f docker-compose-phpmyadmin.yml down

local-env:
	cp .env.example .env

lint:
	uv run ruff check .

tests:
	uv run -m pytest -q

tidy:
	uv run isort bafrapy bafrapy_sqlacodegen scripts flows
	uv run ruff format bafrapy bafrapy_sqlacodegen scripts flows

compile-generator:
	uv pip install -e ./bafrapy_sqlacodegen --force-reinstall

generate-models:
	uv run scripts/generate-models.py

build-directus-sync:
	docker build -t bafrapy-directus-sync ./directus-sync

directus-schema-pull: build-directus-sync
	docker run --rm \
		--user $(shell id -u):$(shell id -g) \
		--env-file .env \
		-v $(PWD)/directus-schema:/workspace/directus-schema \
		--network bafrapy-network \
		bafrapy-directus-sync pull

directus-schema-push: build-directus-sync
	docker run --rm \
		--user $(shell id -u):$(shell id -g) \
		--env-file .env \
		-v $(PWD)/directus-schema:/workspace/directus-schema \
		--network bafrapy-network \
		bafrapy-directus-sync push
