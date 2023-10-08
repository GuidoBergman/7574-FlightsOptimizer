	docker build -f ./server/Dockerfile -t "server:latest" .
	docker build -f ./client/Dockerfile -t "client:latest" .
	docker build -f ./rabbitmq/Dockerfile -t "rabbitmq:latest" .
	docker build -f ./filtros/filtro_escalas/Dockerfile -t "filtro_escalas:latest" .
	docker build -f ./filtros/filtro_velocidad/Dockerfile -t "filtro_velocidad:latest" .
	docker build -f ./filtros/filtro_distancia/Dockerfile -t "filtro_distancia:latest" .
	docker build -f ./filtros/filtro_precio/Dockerfile -t "filtro_precio:latest" .
	docker build -f ./filtros/calculador_promedio/Dockerfile -t "calculador_promedio:latest" .

	
	docker compose -f docker-compose-dev.yaml up -d --build