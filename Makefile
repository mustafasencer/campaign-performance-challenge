.PHONY: dev-deps deps format lint run-infra run-infra-background migrate-scenario-1 migrate-scenario-2 run-scenario-1 run-scenario-2 stop-infra down-infra test test-with-coverage help
.DEFAULT_GOAL := help

SERVICE=aklamio-challenge
IMAGE?=$(SERVICE):latest

## dev-deps: Install python dev packages
dev-deps:
	pip install -r requirements.dev.txt

## deps: Install python packages
deps:
	pip install -r requirements.txt

## format: Format codebase
format:
	autoflake --remove-all-unused-imports --recursive --remove-unused-variables --in-place scenario_1st scenario_2nd tests utils --exclude=__init__.py
	black scenario_1st scenario_2nd tests utils main.py
	isort scenario_1st scenario_2nd tests utils main.py --profile black

## lint: Lint codebase
lint:
	mypy scenario_1st scenario_2nd utils
	black scenario_1st scenario_2nd tests utils main.py --check
	isort scenario_1st scenario_2nd tests utils main.py --check-only --profile black

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
	FILE_PATH=data/aklamio_challenge.json \
	python main.py migrate --scenario 1st

## migrate-scenario-2: Migrate the scenario #2 locally on the host machine
migrate-scenario-2:
	POSTGRES_HOST=localhost \
	POSTGRES_PORT=5432 \
	POSTGRES_DB=sample \
	POSTGRES_USERNAME=postgres \
	POSTGRES_PASSWORD=postgres \
	FILE_PATH=data/aklamio_challenge.json \
	python main.py migrate --scenario 2nd

## run-scenario-1: Run the scenario #1 locally on the host machine
run-scenario-1:
	POSTGRES_HOST=localhost \
	POSTGRES_PORT=5432 \
	POSTGRES_DB=sample \
	POSTGRES_USERNAME=postgres \
	POSTGRES_PASSWORD=postgres \
	FILE_PATH=data/aklamio_challenge_test.json \
	python main.py run --scenario 1st

## run-scenario-2: Run the scenario #2 locally on the host machine
run-scenario-2:
	POSTGRES_HOST=localhost \
	POSTGRES_PORT=5432 \
	POSTGRES_DB=sample \
	POSTGRES_USERNAME=postgres \
	POSTGRES_PASSWORD=postgres \
	FILE_PATH=data/aklamio_challenge.json \
	python main.py run --scenario 2nd

## test: Run the project unit tests
test:
	pytest -v

## test-with-coverage: Run the unit tests and calculate the coverage
test-with-coverage:
	pytest --cov-report term-missing --cov=.

## :
## help: Print out available make targets.
help: Makefile
	@echo
	@echo " Choose a command run:"
	@echo
	@sed -n 's/^##//p' $< | column -t -s ':' |  sed -e 's/^/ /'
	@echo