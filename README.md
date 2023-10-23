# TP Escalabilidad: Middleware y Coordinación de Procesos

La documentación de arquitectura se encuentra en el archivo "Flights Optmizer - Documento de arquitectura".

## Alcance del sistema

El sistema flights optimizer procesa registros de vuelos de avión y a partir de ellos permite conocer:
- Los vuelos con 3 escalas o más
- Los vuelos cuya distancia total es mayor a cuatro veces la distancia directa entre el origen y el destino
- Los 2 vuelos más rápidos por trayecto, es decir origen y destino del vuelo, considerando solo los vuelos con 3 escalas o más
- El precio promedio y máximo para cada trayecto, considerando solamente los vuelos cuyo precio está por encima del - promedio de todos los precios

## Uso 

>python3.exe createDockerCompose.py 2 3 3 3 3

El modo de uso es el siguiente:

  Debe ingresar las cantidades de los filtros que desea crear.
  
  El formato del comando es  'createDockerCompose.py <cant handlers server>  <cant filtro escalas> <cant filtro distancia> <cant filtro rapidos> <cant filtro precio>'
Ej: el comando 'createDockerCompose.py 1 2 3 4 5' creara:
  - 1 handlers para el server
  - 2 filtros de escalas
  - 3 filtros de distancia
  - 4 filtros de vuelos rápidos
  - 5 filtros de estadisticas de precios


Luego se inicia con 

```
build.bat
```
en windows o 

```
make docker-compose-up
```

en Ubuntu


## Archivos y configuración

Los archivos a enviar deben estar en la carpeta /data y definidos a traves del /client/config.ini

Los resultados quedaran en la carpeta /data en los archivos:

- ResultadoFiltroEscalas.txt
- ResultadoEstadisticaPrecios.txt
- ResultadoVuelosRapidos.txt
- ResultadoFiltroDistancia.txt
