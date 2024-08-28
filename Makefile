DOCKER_COMPOSE = docker-compose up -d 

.PHONY: build
build:
	docker-compose up --build

.PHONY: start
start: _start_mysql _start_app _start_admin

.PHONY: stop
stop:
	docker-compose down

_start_mysql:
	$(DOCKER_COMPOSE) mysql-db

_start_app:
	$(DOCKER_COMPOSE) api_server

_start_admin:
	$(DOCKER_COMPOSE) phpmyadmin
