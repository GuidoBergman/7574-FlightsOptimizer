FROM python:alpine
COPY filtros/filtro_escalas /
COPY protocolos /
RUN pip3 install pika
ENTRYPOINT ["/bin/sh"]
