# TP Escalabilidad: Middleware y Coordinaci�n de Procesos

python3.exe createDockerCompose.py 2 3 3 3 3

Alcance del sistema

El sistema flights optimizer procesa registros de vuelos de avi�n y a partir de ellos permite conocer:
Los vuelos con 3 escalas o m�s
Los vuelos cuya distancia total es mayor a cuatro veces la distancia directa entre el origen y el destino
Los 2 vuelos m�s r�pidos por trayecto, es decir origen y destino del vuelo, considerando solo los vuelos con 3 escalas o m�s
El precio promedio y m�ximo para cada trayecto, considerando solamente los vuelos cuyo precio est� por encima del promedio de todos los precios

##Instalacion 

>python3.exe createDockerCompose.py 2 3 3 3 3

El modo de uso es el siguiente:

  Debe ingresar las cantidades de los filtros que desea crear.
    El formato del comando es  'createDockerCompose.py <cant handlers server>  <cant filtro escalas> <cant filtro distancia> <cant filtro rapidos> <cant filtro precio>'
    Ej: el comando 'createDockerCompose.py 1 2 3 4 5' creara:
      - 1 handlers para el server
      - 2 filtros de escalas
      - 3 filtros de distancia
      - 4 filtros de vuelos r�pidos
      - 5 filtros de estadisticas de precios


Luego se inicia con 

> build.bat

en windows o 

> Makefile docker-compose-up

en Ubuntu


## Archivos y configuraci�n

Los archivos a enviar deben estar en la carpeta /data y definidos a traves del /client/config.ini

Los resultados quedaran en la carpeta /data en los archivos:

ResultadoFiltroEscalas.txt
ResultadoEstadisticaPrecios.txt
ResultadoVuelosRapidos.txt
ResultadoFiltroDistancia.txt
