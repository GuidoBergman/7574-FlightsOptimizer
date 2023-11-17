import logging
from struct import unpack, pack, calcsize
from time import sleep
import signal

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

class ProtocoloEnviarHeartbeat:

    def __init__(self, socket, host_watchog, port_watchdog, cant_watchdogs, identificador_tipo, periodo=1, numero=0):
        self._socket = socket
        self._host_watchdog = host_watchog
        self._port_watchdog = port_watchdog
        self._cant_watchdogs = cant_watchdogs
        self._identificador_tipo = identificador_tipo
        self._numero = numero
        self._periodo = periodo
        self._seguir_corriendo = True
    


    def _enviar_heartbeat(self):
        formato_mensaje = FORMATO_MENSAJE
        tamanio_mensaje = calcsize(formato_mensaje)
        msg = pack(formato_mensaje,
            self._identificador_tipo.encode(STRING_ENCODING), self._numero
        )

        for i in range(1, self._cant_watchdogs + 1):
            try:
                estado = self._socket.send(msg, tamanio_mensaje, (self._host_watchdog + str(i), self._port_watchdog))
            except Exception:
                logging.error(f"Error al enviar heartbeat al watchdog {i}")
            if estado == STATUS_ERR:
                logging.error(f"Error al enviar heartbeat al watchdog {i}")
                return STATUS_ERR

        return STATUS_OK

    def enviar_heartbeats(self):
        while self._seguir_corriendo:
            estado = self._enviar_heartbeat()
            sleep(self._periodo)

    def cerrar(self):
        self._socket.close()

    def sigterm_handler(self, _signo, _stack_frame):
        self._seguir_corriendo = False
        self._cerrar()