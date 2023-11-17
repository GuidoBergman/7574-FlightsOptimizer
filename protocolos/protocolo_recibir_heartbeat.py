import logging
from struct import unpack, pack, calcsize


IDENTIFICADOR_SERVER = 'S'
IDENTIFICADOR_FILTRO_VELOCIDAD = 'V'
IDENTIFICADOR_FILTRO_ESCALAS = 'E'
IDENTIFICADOR_FILTRO_DISTANCIA = 'D'
IDENTIFICADOR_FILTRO_PRECIO = 'P'
IDENTIFICADOR_CALCULADOR_PROMEDIO = 'C'
IDENTIFIFICADOR_WATCHDOG = 'W'


STRING_ENCODING = 'utf-8'
FORMATO_MENSAJE = '!cH'


STATUS_OK = 0
STATUS_ERR = -1
STATUS_TIMEOUT = -2

class ProtocoloRecibirHeartbeat:

    def __init__(self, socket):
        self._socket = socket


    def recibir_hearbeat(self):
        formato_mensaje = FORMATO_MENSAJE
        tamanio_mensaje = calcsize(formato_mensaje)
        try:
            estado, mensaje = self._socket.receive(tamanio_mensaje)
        except TimeoutError:
            return STATUS_TIMEOUT, None, None
        if estado != STATUS_OK:
            logging.error(f'Error al recibir heartbeat')
            return STATUS_ERR, None, None

        identificador_tipo, numero = unpack(formato_mensaje, mensaje)
        identificador_tipo = identificador_tipo.decode(STRING_ENCODING)

        # Si el que envió el heartbeat no tiene numero
        if numero == 0:
            return STATUS_OK, identificador_tipo, None

        return STATUS_OK, identificador_tipo, numero



    def cerrar(self):
        self._socket.close()

   