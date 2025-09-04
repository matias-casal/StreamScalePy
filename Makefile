.PHONY: help build up down restart logs clean test lint format install

# Variables
DOCKER_COMPOSE = docker-compose
PYTHON = python3
PIP = pip3

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install: ## Install Python dependencies
	$(PIP) install -r requirements.txt

build: ## Build Docker images
	$(DOCKER_COMPOSE) build

up: ## Start all services
	$(DOCKER_COMPOSE) up -d

down: ## Stop all services
	$(DOCKER_COMPOSE) down

restart: ## Restart all services
	$(DOCKER_COMPOSE) restart

logs: ## View logs from all services
	$(DOCKER_COMPOSE) logs -f

logs-api: ## View API logs
	$(DOCKER_COMPOSE) logs -f api

logs-scheduler: ## View scheduler logs
	$(DOCKER_COMPOSE) logs -f scheduler

logs-worker: ## View worker logs
	$(DOCKER_COMPOSE) logs -f worker

status: ## Show service status
	$(DOCKER_COMPOSE) ps

clean: ## Clean up containers, volumes, and networks
	$(DOCKER_COMPOSE) down -v
	docker system prune -f

test: ## Run tests
	$(PYTHON) -m pytest src/tests/ -v --cov=src --cov-report=term-missing

test-docker: ## Run tests in Docker container
	docker exec streamscale_api pytest src/tests/ -v

shell-api: ## Open shell in API container
	docker exec -it streamscale_api /bin/bash

shell-postgres: ## Open PostgreSQL shell
	docker exec -it streamscale_postgres psql -U streamscale_user -d streamscale

shell-redis: ## Open Redis CLI
	docker exec -it streamscale_redis redis-cli

rabbitmq-ui: ## Open RabbitMQ Management UI
	@echo "Opening RabbitMQ Management UI at http://localhost:15672"
	@echo "Default credentials: guest/guest"

scale-workers: ## Scale worker service (usage: make scale-workers n=4)
	$(DOCKER_COMPOSE) up -d --scale worker=$(n)


backup-db: ## Backup PostgreSQL database
	docker exec streamscale_postgres pg_dump -U streamscale_user streamscale > backup_$(shell date +%Y%m%d_%H%M%S).sql