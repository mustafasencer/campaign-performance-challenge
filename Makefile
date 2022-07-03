.PHONY: deps run-infra run-infra-background migrate-scenario-1 migrate-scenario-2 run-scenario-1 run-scenario-2 stop-infra down-infra test test-with-coverage help
.DEFAULT_GOAL := help

SERVICE=etl
IMAGE?=$(SERVICE):latest

## dev-deps: Install python dev packages
dev-deps:
	pip install -r requirements.dev.txt

## deps: Install python packages
deps:
	pip install -r requirements.txt

## run-infra: Run the infrastructure services required for the service
run-infra:
	docker-compose -f docker-compose.yml up

## run-infra-background: Run the infrastructure services required for the service in the background
run-infra-background:
	docker-compose -f docker-compose.yml up -d

stop-infra:
	docker-compose -f docker-compose.yml stop

## down-infra: Stop the infrastructure services running in docker and remove orphans
down-infra:
	docker-compose -f docker-compose.yml down --remove-orphans

## migrate-scenario-1: Migrate the scenario #1 locally on the host machine
migrate-scenario-1:
	POSTGRES_HOST=localhost \
	POSTGRES_PORT=5432 \
	POSTGRES_DB=sample \
	POSTGRES_USERNAME=postgres \
	POSTGRES_PASSWORD=postgres \
	python main.py migrate 1

## migrate-scenario-2: Migrate the scenario #2 locally on the host machine
migrate-scenario-2:
	POSTGRES_HOST=localhost \
	POSTGRES_PORT=5432 \
	POSTGRES_DB=sample \
	POSTGRES_USERNAME=postgres \
	POSTGRES_PASSWORD=postgres \
	python main.py migrate 2

## run-scenario-1: Run the scenario #1 locally on the host machine
run-scenario-1:
	POSTGRES_HOST=localhost \
	POSTGRES_PORT=5432 \
	POSTGRES_DB=sample \
	POSTGRES_USERNAME=postgres \
	POSTGRES_PASSWORD=postgres \
	python main.py run 1

## run-scenario-2: Run the scenario #2 locally on the host machine
run-scenario-2:
	POSTGRES_HOST=localhost \
	POSTGRES_PORT=5432 \
	POSTGRES_DB=sample \
	POSTGRES_USERNAME=postgres \
	POSTGRES_PASSWORD=postgres \
	python main.py run 2

## test: Run the project unit tests
test:
	pytest -v

## test-with-coverage: Run the unit tests and calculate the coverage
test-with-coverage:
	pytest --cov-report term-missing --cov=etl tests/

## :
## help: Print out available make targets.
help: Makefile
	@echo
	@echo " Choose a command run:"
	@echo
	@sed -n 's/^##//p' $< | column -t -s ':' |  sed -e 's/^/ /'
	@echo