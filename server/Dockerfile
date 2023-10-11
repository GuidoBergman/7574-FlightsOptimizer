FROM python:3.9.7-slim
COPY server /
COPY protocolos /
RUN pip3 install pika
ENTRYPOINT ["/bin/sh"]
