
serve:
	docker compose down
	docker compose up -d

serve-down:
	docker compose down

phpmyadmin:
	docker compose -f docker-compose-phpmyadmin.yml down
	docker compose -f docker-compose-phpmyadmin.yml up -d

phpmyadmin-down:
	docker compose -f docker-compose-phpmyadmin.yml down

ch-log-error:
	docker exec -it bafrapy_clickhouse cat /var/log/clickhouse-server/clickhouse-server.err.log

worker-dev:
	echo DATA BACKTEST ASSETS| xargs -n1 -P3 -I{} bash -c 'rq worker {} --worker-class rq.SimpleWorker --url redis://localhost:6379/0'

start-worker:
	python bafrapy/worker_main.py $(QUEUE)

local-env:
	cp .env.example .env

lint:
	ruff check .

tidy:
	isort bafrapy

compile-generator:
	uv pip install -e ./bafrapy_sqlcodegen --force-reinstall

generate-models:
	uv run scripts/generate-models.py