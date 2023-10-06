from modelo.ResultadoEstadisticasPrecios import ResultadoEstadisticaPrecios
from modelo.ResultadoFiltroDistancia import ResultadoFiltroDistancia
from modelo.ResultadoFiltroEscalas import ResultadoFiltroEscalas
from modelo.ResultadoVuelosRapidos import ResultadoVuelosRapidos
from modelo.Vuelo import Vuelo
from socket_comun import SocketComun, STATUS_ERR, STATUS_OK
import logging

TAMANIO_IDENTIFICADOR_RESULTADO = 1
IDENTIFICADOR_RESULTADO_RAPIDOS = 'R'
IDENTIFICADOR_RESULTADO_DISTANCIA = 'D'
IDENTIFICADOR_RESULTADO_ESCALAS = 'E'
IDENTIFICADOR_RESULTADO_PRECIO = 'P'
IDENTIFICADOR_FIN_RAPIDOS = 'r'
IDENTIFICADOR_FIN_DISTANCIA = 'd'
IDENTIFICADOR_FIN_ESCALAS = 'e'
IDENTIFICADOR_FIN_PRECIO = 'p'
STRING_ENCODING = 'utf-8'

class ProtocoloResultadosCliente:

    def __init__(self, socket):
        self._socket = socket


    def enviar_resultado_vuelos_rapidos(self, resultado: ResultadoVuelosRapidos):
        print()

    def enviar_resultado_filtro_distancia(self, resultado: ResultadoFiltroDistancia):
        print()
        
    def enviar_resultado_filtro_escalas(self, resultado: ResultadoFiltroEscalas):
        estado = self._socket.send(IDENTIFICADOR_RESULTADO_ESCALAS.encode(STRING_ENCODING), TAMANIO_IDENTIFICADOR_RESULTADO)
        if estado == STATUS_ERR:
            logging.error("acción: enviar_resultado_filtro_escalas | resultado: error")
            return STATUS_ERR

        tamanio, mensaje = resultado.serializar()
        estado = self._socket.send(mensaje, tamanio)
        if estado == STATUS_ERR:
            logging.error("acción: enviar_resultado_filtro_escalas | resultado: error")
            return STATUS_ERR

        return STATUS_OK

    def enviar_resultado_filtro_precio(self, resultado: ResultadoEstadisticaPrecios):
        print()


       
    def recibir_resultado(self):
        estado, mensaje, _ = self._socket.receive(TAMANIO_IDENTIFICADOR_RESULTADO)
        if estado != STATUS_OK:
            return STATUS_ERR, None
        
        identificador_resultado =  mensaje.decode(STRING_ENCODING)

        if identificador_resultado == IDENTIFICADOR_RESULTADO_ESCALAS:
            resultado = ResultadoFiltroEscalas
        else:
            return STATUS_ERR, None
        
        tamanio_mensaje = resultado.calcular_tamanio()
        estado, mensaje, _ = self._socket.receive(tamanio_mensaje)
        if estado != STATUS_OK:
            return STATUS_ERR, None

        return STATUS_OK, resultado.deserializar(mensaje)