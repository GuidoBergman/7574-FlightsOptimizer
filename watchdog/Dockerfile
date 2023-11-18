FROM python:alpine
COPY watchdog /
COPY protocolos /
RUN apk update
RUN apk add docker
ENTRYPOINT ["/bin/sh"]
