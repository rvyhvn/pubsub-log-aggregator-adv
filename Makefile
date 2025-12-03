.PHONY: help build up down logs test clean stats health demo

help:
	@echo "Available commands:"
	@echo "  make build    - Build all Docker images"
	@echo "  make up       - Start all services"
	@echo "  make down     - Stop all services"
	@echo "  make logs     - View aggregator logs"
	@echo "  make test     - Run all tests"
	@echo "  make clean    - Remove containers and volumes"
	@echo "  make stats    - Show current statistics"
	@echo "  make health   - Check service health"
	@echo "  make demo     - Run complete demo"

build:
	@echo "Building Docker images..."
	docker compose build

up:
	@echo "Starting services..."
	docker compose up -d
	@echo "Waiting for services to be healthy..."
	@sleep 10
	@echo "Services started! Access API at http://localhost:8080"

down:
	@echo "Stopping services..."
	docker compose down

logs:
	@echo "Tailing aggregator logs (Ctrl+C to exit)..."
	docker compose logs -f aggregator

logs-all:
	@echo "Tailing all service logs..."
	docker compose logs -f

test:
	@echo "Running test suite..."
	docker compose run --rm aggregator pytest -v --tb=short

test-dedup:
	@echo "Running deduplication tests..."
	docker compose run --rm aggregator pytest tests/test_dedup.py -v

test-concurrency:
	@echo "Running concurrency tests..."
	docker compose run --rm aggregator pytest tests/test_concurrency.py -v

test-api:
	@echo "Running API tests..."
	docker compose run --rm aggregator pytest tests/test_api.py -v

test-persistence:
	@echo "Running persistence tests..."
	docker compose run --rm aggregator pytest tests/test_persistence.py -v

clean:
	@echo "Cleaning up containers and volumes..."
	docker compose down -v
	@echo "Cleanup complete!"

stats:
	@echo "Fetching statistics..."
	@curl -s http://localhost:8080/stats | python3 -m json.tool

health:
	@echo "Checking service health..."
	@curl -s http://localhost:8080/health | python3 -m json.tool

topics:
	@echo "Fetching topics..."
	@curl -s http://localhost:8080/topics | python3 -m json.tool

events:
	@echo "Fetching recent events (limit 10)..."
	@curl -s "http://localhost:8080/events?limit=10" | python3 -m json.tool

demo: clean build up
	@echo "=== DEMO: Distributed Pub-Sub Aggregator ==="
	@echo ""
	@echo "1. Waiting for services to start..."
	@sleep 15
	@echo ""
	@echo "2. Checking health..."
	@make health
	@echo ""
	@echo "3. Waiting for publisher to generate events..."
	@sleep 30
	@echo ""
	@echo "4. Statistics:"
	@make stats
	@echo ""
	@echo "5. Topics:"
	@make topics
	@echo ""
	@echo "6. Recent events:"
	@make events
	@echo ""
	@echo "7. Testing persistence (restarting services)..."
	@docker compose restart aggregator
	@sleep 10
	@echo ""
	@echo "8. Statistics after restart (should be same):"
	@make stats
	@echo ""
	@echo "=== DEMO COMPLETE ==="
	@echo "Run 'make logs' to view live logs"
	@echo "Run 'make test' to run test suite"

verify-persistence:
	@echo "=== Testing Persistence ==="
	@echo "1. Getting current stats..."
	@make stats > /tmp/stats_before.txt
	@echo ""
	@echo "2. Stopping services..."
	@docker compose down
	@echo ""
	@echo "3. Restarting services..."
	@docker compose up -d
	@sleep 15
	@echo ""
	@echo "4. Getting stats after restart..."
	@make stats > /tmp/stats_after.txt
	@echo ""
	@echo "5. Comparing stats..."
	@diff /tmp/stats_before.txt /tmp/stats_after.txt && echo "✅ Data persisted correctly!" || echo "❌ Data mismatch!"

stress-test:
	@echo "=== Running Stress Test ==="
	@echo "Sending 1000 additional events via API..."
	@for i in {1..1000}; do \
		curl -s -X POST http://localhost:8080/publish \
			-H "Content-Type: application/json" \
			-d '{"topic":"stress.test","event_id":"stress_'$$i'","timestamp":"2025-12-02T10:30:00Z","source":"stress","payload":{"index":'$$i'}}' \
			> /dev/null; \
	done
	@echo "Stress test complete! Check stats:"
	@make stats

shell-aggregator:
	@echo "Opening shell in aggregator container..."
	docker compose exec aggregator /bin/bash

shell-db:
	@echo "Opening PostgreSQL shell..."
	docker compose exec storage psql -U agguser -d aggregator_db

db-query-events:
	@echo "Querying processed events (limit 10)..."
	docker compose exec storage psql -U agguser -d aggregator_db -c "SELECT topic, event_id, source, processed_at FROM processed_events ORDER BY processed_at DESC LIMIT 10;"

db-query-stats:
	@echo "Querying statistics..."
	docker compose exec storage psql -U agguser -d aggregator_db -c "SELECT * FROM event_stats;"

db-query-duplicates:
	@echo "Finding duplicate attempts in audit logs..."
	docker compose exec storage psql -U agguser -d aggregator_db -c "SELECT event_topic, event_id, COUNT(*) as attempts FROM audit_logs WHERE action='duplicate' GROUP BY event_topic, event_id ORDER BY attempts DESC LIMIT 10;"
