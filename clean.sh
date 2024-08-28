#!/bin/bash

docker-compose down -v
docker-compose rm

docker rmi mysql:5.7 api_server_t2_img:latest \
        phpmyadmin/phpmyadmin -f

sudo rm -rf ./database/mysql-db/
