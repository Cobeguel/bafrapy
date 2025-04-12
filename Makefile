
serve:
	docker compose up -d

serve-down:
	docker compose down

phpmyadmin: 
	docker compose -f docker-compose-dev.yml up -d

phpmyadmin-down:
	docker compose -f docker-compose-phpmyadmin.yml down

ch-log-error:
	docker exec -it bafrapy_clickhouse cat /var/log/clickhouse-server/clickhouse-server.err.log

tidy:
	isort bafrapy