.PHONY: up down test sim

up:
	docker compose up --build

down:
	docker compose down -v

test:
	PYTHONPATH=src pytest -q

sim:
	PYTHONPATH=src python3 -m kafka_demo.entrypoints.sim_server
