from math import log
from struct import calcsize, unpack
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

    def enviar_resultados(self, msjResultados):
        logging.info(f"Envia resultado")
        self._socket.send(msjResultados, len(msjResultados))
        
    def enviar_resultado_vuelos_rapidos(self, resultado: ResultadoVuelosRapidos):
        return self._enviar_resultado(resultado, 'vuelos_rapidos', IDENTIFICADOR_RESULTADO_RAPIDOS)

    def enviar_resultado_filtro_distancia(self, resultado: ResultadoFiltroDistancia):
        return self._enviar_resultado(resultado, 'filtro_distancia', IDENTIFICADOR_RESULTADO_DISTANCIA)
        
    def enviar_resultado_filtro_escalas(self, resultado: ResultadoFiltroEscalas):
        return self._enviar_resultado(resultado, 'filtro_escalas', IDENTIFICADOR_RESULTADO_ESCALAS)

    def enviar_resultado_filtro_precio(self, resultado: ResultadoEstadisticaPrecios):
        return self._enviar_resultado(resultado, 'estadisticas_precios', IDENTIFICADOR_RESULTADO_PRECIO)

    def _enviar_resultado(self, resultado, nombre_resultado, identificador_resultado):
        estado = self._socket.send(identificador_resultado.encode(STRING_ENCODING), TAMANIO_IDENTIFICADOR_RESULTADO)
        if estado == STATUS_ERR:
            logging.error(f"acción: enviar_resultado_{nombre_resultado} | resultado: error")
            return STATUS_ERR

        tamanio, mensaje = resultado.serializar()
        estado = self._socket.send(mensaje, tamanio)
        if estado == STATUS_ERR:
            logging.error(f"acción: enviar_resultado_{nombre_resultado} | resultado: error")
            return STATUS_ERR

        return STATUS_OK

    def enviar_fin_resultados_rapidos(self):
        return self._enviar_fin_resultados(IDENTIFICADOR_FIN_RAPIDOS, 'vuelos_rapidos')

    def enviar_fin_resultados_distancia(self):
        return self._enviar_fin_resultados(IDENTIFICADOR_FIN_DISTANCIA, 'filtro_distancia')

    def enviar_fin_resultados_escalas(self):
        return self._enviar_fin_resultados(IDENTIFICADOR_FIN_ESCALAS, 'filtro_escalas')
    
    def enviar_fin_resultados_filtro_precio(self):
        return self._enviar_fin_resultados(IDENTIFICADOR_FIN_PRECIO, 'estadisticas_precios')


    def _enviar_fin_resultados(self, identificador_fin, nombre_resultado):
        estado = self._socket.send(identificador_fin.encode(STRING_ENCODING), TAMANIO_IDENTIFICADOR_RESULTADO)
        if estado == STATUS_ERR:
            logging.error(f"acción: enviar_fin_resultados_{nombre_resultado} | resultado: error")
            return STATUS_ERR
        return STATUS_OK


       
    def recibir_resultado(self):
        
        logging.info("Recubi tipo mensaje")
        estado, mensaje = self._socket.receive(TAMANIO_IDENTIFICADOR_RESULTADO)
        if estado != STATUS_OK:
            return STATUS_ERR, None
        
        identificador_resultado =  mensaje.decode(STRING_ENCODING)
        
        logging.info(f"Mensaje del tipo {identificador_resultado}")
        if identificador_resultado == IDENTIFICADOR_FIN_RAPIDOS:
            return IDENTIFICADOR_FIN_RAPIDOS, None
        elif identificador_resultado == IDENTIFICADOR_FIN_DISTANCIA:
            return IDENTIFICADOR_FIN_DISTANCIA, None
        elif identificador_resultado == IDENTIFICADOR_FIN_ESCALAS:
            return IDENTIFICADOR_FIN_ESCALAS, None
        elif identificador_resultado == IDENTIFICADOR_FIN_PRECIO:
            return IDENTIFICADOR_FIN_PRECIO, None
        
        
        #Si llega aca es porque son resultados, entonces recibe la cantidad:
        resultados = []
        logging.info("Llegaron resultados asi que voy a ver cuantos son")
        estado, mensaje = self._socket.receive(calcsize("!H"))
        if estado != STATUS_OK:
            return STATUS_ERR, None
        total_resultados = unpack("!H", mensaje)
        if type(total_resultados) is tuple:
            total_resultados = total_resultados[0] 
            
        logging.info(f"Llegaron {total_resultados} resultados")
        
        for i in range(1, total_resultados + 1):
            if identificador_resultado == IDENTIFICADOR_RESULTADO_RAPIDOS:
                resultado = ResultadoVuelosRapidos
            elif identificador_resultado == IDENTIFICADOR_RESULTADO_DISTANCIA:
                resultado = ResultadoFiltroDistancia
            elif identificador_resultado == IDENTIFICADOR_RESULTADO_ESCALAS:
                resultado = ResultadoFiltroEscalas
            elif identificador_resultado == IDENTIFICADOR_RESULTADO_PRECIO:
                resultado = ResultadoEstadisticaPrecios
            tamanio_mensaje = resultado.calcular_tamanio()
            estado, mensaje = self._socket.receive(tamanio_mensaje)
            if estado != STATUS_OK:
                return STATUS_ERR, None        
            resultados.append(resultado.deserializar(mensaje))
        return STATUS_OK, resultados
    def cerrar(self):
        self._socket.close()