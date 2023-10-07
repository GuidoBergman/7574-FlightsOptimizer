FROM python:alpine
COPY filtros/filtro_velocidad /
COPY protocolos /
RUN pip3 install pika
ENTRYPOINT ["/bin/sh"]
