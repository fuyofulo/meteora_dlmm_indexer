.PHONY: up down schema run-indexer run-api dashboard-install dashboard-dev dashboard-build demo smoke check test fmt clippy load

up:
	docker compose -f docker-compose.clickhouse.yml up -d

down:
	docker compose -f docker-compose.clickhouse.yml down

schema:
	docker exec -i dune-project-clickhouse clickhouse-client \
		--user dune_project --password dune_project_pass --multiquery \
		< schema/clickhouse_v2.sql

run-indexer:
	cargo run -p indexer

run-api:
	cargo run -p dune-project-api

dashboard-install:
	cd dashboard && npm install

dashboard-dev:
	cd dashboard && npm run dev

dashboard-build:
	cd dashboard && npm run build

fmt:
	cargo fmt --all

clippy:
	cargo clippy --workspace --all-targets

check:
	cargo check --workspace

test:
	cargo test --workspace
