FROM python:alpine
COPY filtros/filtro_distancia /
COPY protocolos /
RUN pip3 install geopy
RUN pip3 install pika
ENTRYPOINT ["/bin/sh"]
