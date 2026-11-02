# ==============================================================================
# Autonomous Procurement Multi-Agent System - Makefile
# ==============================================================================

.PHONY: help up down restart start run consumer slack test-event test-event-healthy test install status clean

PYTHON ?= .venv/bin/python
PIP ?= .venv/bin/pip
PYTEST ?= .venv/bin/pytest

help:
	@echo "======================================================================"
	@echo " 🚀 Autonomous Procurement Multi-Agent System"
	@echo "======================================================================"
	@echo "  make up                 Start Docker infrastructure (Apache Kafka, PostgreSQL)"
	@echo "  make down               Stop Docker containers"
	@echo "  make restart            Restart Docker containers"
	@echo "  make start              Start all agents & Dashboard (Agent Hub & ERP on :8002, Kafka, Slack) via main.py"
	@echo "  make erp                Start Agent Hub (Real-Time Dashboard & Embedded ERP) on :8002"
	@echo "  make consumer           Start Kafka consumer worker daemon only"
	@echo "  make slack              Start Slack Socket Mode listener daemon only"
	@echo "  make test-event         Trigger low-stock test event (TSHIRT-BLK-M)"
	@echo "  make test-event-healthy Trigger healthy-stock event (pre-filter zero tokens)"
	@echo "  make test               Run full test suite (pytest -v)"
	@echo "  make install            Set up virtual environment & install dependencies"
	@echo "  make status             Check Docker containers and Mock ERP UI status"
	@echo "  make clean              Remove Python cache files"
	@echo "======================================================================"

# Docker Infrastructure
up:
	docker compose up -d

down:
	docker compose down

restart:
	docker compose restart

# Agent Daemons & Unified System
start:
	$(PYTHON) -m src.main

run: start

erp:
	$(PYTHON) -m uvicorn src.server.app:app --host 0.0.0.0 --port 8002

consumer:
	$(PYTHON) -m src.workers.kafka_consumer

slack:
	$(PYTHON) -m src.workers.slack_listener

# Test Events (Event Ingestion)
test-event:
	$(PYTHON) -m src.producer --sku TSHIRT-BLK-M --stock 4 --threshold 10

test-event-healthy:
	$(PYTHON) -m src.producer --sku TSHIRT-BLK-M --stock 50 --threshold 10

# Testing & Quality
test:
	$(PYTEST) -v

# Setup & Maintenance
install:
	python3 -m venv .venv
	$(PIP) install --upgrade pip setuptools wheel
	$(PIP) install -e ".[dev]"

status:
	@echo "=== Docker Services ==="
	@docker compose ps
	@echo ""
	@echo "=== Agent Hub & Real-Time Traceability Health Check ==="
	@curl -s http://localhost:8002/health || echo "Agent Hub not reachable on port 8002"
	@echo ""

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
