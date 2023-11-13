up:
	docker-compose --file etc/docker/dev/docker-compose.yml --profile storage --profile monitoring up -d

down:
	docker-compose --file etc/docker/dev/docker-compose.yml --profile storage --profile monitoring down

exec:
	docker exec -it dev-rucio-1 /bin/bash

test-service-port:
	docker-compose --file etc/docker/dev/docker-compose.yml --profile storage run ruciodb -d --service-ports

test-manual-port:
	docker-compose --file etc/docker/dev/docker-compose.yml --profile storage run -p 127.0.0.1:5432:5432 ruciodb