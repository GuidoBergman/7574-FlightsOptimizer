	docker build -f ./filtros/filtro_escalas/Dockerfile -t "filtro_escalas:latest" .
	docker build -f ./filtros/filtro_velocidad/Dockerfile -t "filtro_velocidad:latest" .
	docker build -f ./filtros/filtro_distancia/Dockerfile -t "filtro_distancia:latest" .

	docker compose -f docker-compose-dev.yaml up -d --build