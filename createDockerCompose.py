import sys

FILEPATH='docker-compose-dev.yaml'


if len(sys.argv) < 6 or sys.argv[1] == '-h':
    print("""
    Debe ingresar las cantidades de los filtros que desea crear. 
    El formato del comando es  \'createDockerCompose.py <cant handlers server>  <cant filtro escalas> <cant filtro distancia> <cant filtro rapidos> <cant filtro precio> <cant watchdogs>\'
    Ej: el comando \'createDockerCompose.py 1 2 3 4 5 6\' creara:
      - 1 handlers para el server
      - 2 filtros de escalas
      - 3 filtros de distancia
      - 4 filtros de vuelos rápidos
      - 5 filtros de estadisticas de precios
      - 6 watchdogs
    """)
    exit()


cantHandlers = int(sys.argv[1])
cantEscalas = int(sys.argv[2])
cantDistancias = int(sys.argv[3])
cantRapidos = int(sys.argv[4])
cantPrecios = int(sys.argv[5])
cantWatchdogs = int(sys.argv[6])
    


# Define the text of the Docker Compose file
textInitialConfig = """version: '3.9'
name: tp1
services:
"""

textRabbitConfig = """
  rabbitmq:
    build:
      context: ./rabbitmq
      dockerfile: Dockerfile
    ports:
      - 15672:15672
    healthcheck:
      test: rabbitmq-diagnostics check_port_connectivity
      interval: 5s
      timeout: 3s
      retries: 10
      start_period: 50s
    networks:
      - rabbit_net
    logging:
      driver: "none"
"""
serverDependencies = ''
for i in range(1, cantEscalas+1):
    serverDependencies+= f'      - filtro_escalas{i}\n'
for i in range(1, cantDistancias+1):
    serverDependencies+= f'      - filtro_distancia{i}\n'
for i in range(1, cantRapidos+1):
    serverDependencies+= f'      - filtro_velocidad{i}\n'
for i in range(1, cantPrecios+1):
    serverDependencies+= f'      - filtro_precio{i}\n'

server = f"""
  server:
    container_name: server
    image: server:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
      - LOGGING_LEVEL=INFO
      - CANT_HANDLERS={cantHandlers}
      - CANT_FILTROS_ESCALAS={cantEscalas}
      - CANT_FILTROS_DISTANCIA={cantDistancias}
      - CANT_FILTROS_VELOCIDAD={cantRapidos}
      - CANT_FILTROS_PRECIO={cantPrecios}
      - CANT_WATCHDOGS={cantWatchdogs}
    depends_on:
{serverDependencies}
    networks:
      - testing_net
      - rabbit_net
    volumes:
      - type: bind
        source: ./server/config.ini
        target: /config.ini
"""


textNetworkConfig = """
networks:
  testing_net:
    ipam:
      driver: default
      config:
        - subnet: 168.25.125.0/24
  rabbit_net:
    ipam:
      driver: default
      config:
        - subnet: 128.25.125.0/24
"""

filtros = ''

for i in range(1, cantEscalas+1):
    filtros += f"""
  filtro_escalas{i}:
    container_name: filtro_escalas{i}
    image: filtro_escalas:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
      - LOGGING_LEVEL=INFO
      - ID={i}
      - CANT_FILTROS_VELOCIDAD={cantRapidos}
      - CANT_WATCHDOGS={cantWatchdogs}
    depends_on:
      rabbitmq:
         condition: service_healthy
    networks:
      - rabbit_net
    volumes:
      - type: bind
        source: ./filtros/filtro_escalas/config.ini
        target: /config.ini
    """
for i in range(1, cantDistancias+1):
    filtros += f"""
  filtro_distancia{i}:
    container_name: filtro_distancia{i}
    image: filtro_distancia:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
      - LOGGING_LEVEL=INFO
      - ID={i}
      - CANT_WATCHDOGS={cantWatchdogs}
    depends_on:
      rabbitmq:
         condition: service_healthy
    networks:
      - rabbit_net
    volumes:
      - type: bind
        source: ./filtros/filtro_distancia/config.ini
        target: /config.ini

    """
for i in range(1, cantRapidos+1):
    filtros += f"""
  filtro_velocidad{i}:
    container_name: filtro_velocidad{i}
    image: filtro_velocidad:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
      - LOGGING_LEVEL=INFO
      - ID={i}
      - CANT_FILTROS_ESCALAS={cantEscalas}
      - CANT_WATCHDOGS={cantWatchdogs}
    depends_on:
      - calculador_promedio
    networks:
      - rabbit_net
    volumes:
      - type: bind
        source: ./filtros/filtro_velocidad/config.ini
        target: /config.ini
    """
for i in range(1, cantPrecios+1):
    filtros += f"""
  filtro_precio{i}:
    container_name: filtro_precio{i}
    image: filtro_precio:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
      - LOGGING_LEVEL=INFO
      - ID={i}
      - CANT_WATCHDOGS={cantWatchdogs}
    depends_on:
      rabbitmq:
         condition: service_healthy
    networks:
      - rabbit_net
    volumes:
      - type: bind
        source: ./filtros/filtro_precio/config.ini
        target: /config.ini
    """

watchdogs=''
for i in range(1, cantWatchdogs+1):
  watchdogs += f"""
  watchdog{i}:
    container_name: watchdog{i}
    image: watchdog:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
      - LOGGING_LEVEL=INFO      
      - CANT_FILTROS_ESCALAS={cantEscalas}
      - CANT_FILTROS_DISTANCIA={cantDistancias}
      - CANT_FILTROS_VELOCIDAD={cantRapidos}
      - CANT_FILTROS_PRECIO={cantPrecios}
      - CANT_WATCHDOGS={cantWatchdogs}
      - ID={i}
    depends_on:
      rabbitmq:
         condition: service_healthy
    networks:
      - rabbit_net
    volumes:
      - type: bind
        source: ./watchdog/config.ini
        target: /config.ini
"""


cliente = """
  client:
    container_name: client
    image: client:latest
    entrypoint: python3 /main.py 
    environment:
      - PYTHONUNBUFFERED=1
      - LOGGING_LEVEL=INFO
    networks:
      - testing_net
    depends_on:
      - server
    
    volumes:
      - type: bind
        source: ./client/config.ini
        target: /config.ini
      - type: bind
        source: ./data
        target: /data
"""

calculadorPromedio = f"""
  calculador_promedio:
    container_name: calculador_promedio
    image: calculador_promedio:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
      - LOGGING_LEVEL=INFO      
      - CANT_FILTROS_PRECIO={cantPrecios}
      - CANT_WATCHDOGS={cantWatchdogs}
    depends_on:
      rabbitmq:
         condition: service_healthy
    networks:
      - rabbit_net
    volumes:
      - type: bind
        source: ./filtros/calculador_promedio/config.ini
        target: /config.ini
"""

dockerComposeFile = open(FILEPATH, 'w')
fileContent = [textInitialConfig, textRabbitConfig, server, filtros, calculadorPromedio,
                cliente, watchdogs, textNetworkConfig]

dockerComposeFile.writelines(fileContent)
 

dockerComposeFile.close()