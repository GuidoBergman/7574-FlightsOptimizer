from protocolo_resultados_cliente import ProtocoloResultadosCliente
from manejador_colas import ManejadorColas
import logging
from modelo.ResultadoEstadisticasPrecios import ResultadoEstadisticaPrecios
from modelo.ResultadoFiltroDistancia import ResultadoFiltroDistancia
from modelo.ResultadoFiltroEscalas import ResultadoFiltroEscalas
from modelo.ResultadoVuelosRapidos import ResultadoVuelosRapidos
from modelo.Vuelo import Vuelo
from protocolo_resultados_cliente import (TAMANIO_IDENTIFICADOR_RESULTADO,
                                        IDENTIFICADOR_RESULTADO_RAPIDOS,
                                        IDENTIFICADOR_RESULTADO_DISTANCIA,
                                        IDENTIFICADOR_RESULTADO_ESCALAS, 
                                        IDENTIFICADOR_RESULTADO_PRECIO, 
                                        IDENTIFICADOR_FIN_RAPIDOS, 
                                        IDENTIFICADOR_FIN_DISTANCIA,
                                        IDENTIFICADOR_FIN_ESCALAS,
                                        IDENTIFICADOR_FIN_PRECIO)

HOST_COLAS = 'rabbitmq'
COLA_RESULTADOS = 'cola_resultados'
STRING_ENCODING = 'utf-8'

class ProtocoloResultadosServidor:

    def __init__(self):
       self._colas = ManejadorColas(HOST_COLAS)
       self._corriendo = False
       self._nombre_cola = COLA_RESULTADOS

    # Escuchar los resultados en la cola de resultados del servidor y mandarselos al cliente   
    def iniciar(self, socket_cliente, cant_filtros_escalas,
    cant_filtros_distancia, cant_filtros_velocidad, cant_filtros_precio):      
        self._corriendo = True
        self._protocolo_resultados_cliente = ProtocoloResultadosCliente(socket_cliente)
        

        self._cant_filtros_escalas = cant_filtros_escalas
        self._cant_filtros_distancia = cant_filtros_distancia
        self._cant_filtros_velocidad = cant_filtros_velocidad
        self._cant_filtros_precio = cant_filtros_precio

        self._cant_fines_resultados = {
            'escalas': 0, 'distancia': 0, 'velocidad': 0, 'precio': 0
        }

        self._colas.crear_cola(self._nombre_cola)
        self._colas.consumir_mensajes(self._nombre_cola, self._callback_function)
        

    def parar(self):        
        self._corriendo = False
        self._colas.dejar_de_consumir()

    
    def _callback_function(self, body):
        identificador_resultado = body[0:1].decode(STRING_ENCODING)

        if identificador_resultado == IDENTIFICADOR_RESULTADO_RAPIDOS:
            resultado = ResultadoVuelosRapidos.deserializar(body[1:])
            self._protocolo_resultados_cliente.enviar_resultado_vuelos_rapidos(resultado)
        elif identificador_resultado == IDENTIFICADOR_RESULTADO_DISTANCIA:
            resultado = ResultadoFiltroDistancia.deserializar(body[1:])
            self._protocolo_resultados_cliente.enviar_resultado_filtro_distancia(resultado)
        elif identificador_resultado == IDENTIFICADOR_RESULTADO_ESCALAS:
            resultado = ResultadoFiltroEscalas.deserializar(body[1:])
            self._protocolo_resultados_cliente.enviar_resultado_filtro_escalas(resultado)
        elif identificador_resultado == IDENTIFICADOR_RESULTADO_PRECIO:
            resultado = ResultadoEstadisticaPrecios.deserializar(body[1:])
            self._protocolo_resultados_cliente.enviar_resultado_filtro_precio(resultado)
        elif identificador_resultado == IDENTIFICADOR_FIN_RAPIDOS:
            self._cant_fines_resultados['velocidad'] += 1
            if self._cant_fines_resultados['velocidad'] >= self._cant_filtros_velocidad:
                self._protocolo_resultados_cliente.enviar_fin_resultados_rapidos()
        elif identificador_resultado == IDENTIFICADOR_FIN_DISTANCIA:
            self._cant_fines_resultados['distancia'] += 1
            if self._cant_fines_resultados['distancia'] >= self._cant_filtros_distancia:
                self._protocolo_resultados_cliente.enviar_fin_resultados_distancia()
        elif identificador_resultado == IDENTIFICADOR_FIN_ESCALAS:
            self._cant_fines_resultados['escalas'] += 1
            if self._cant_fines_resultados['escalas'] >= self._cant_filtros_escalas:
                self._protocolo_resultados_cliente.enviar_fin_resultados_escalas()
        elif identificador_resultado == IDENTIFICADOR_FIN_PRECIO:
            self._cant_fines_resultados['precio'] += 1
            if self._cant_fines_resultados['precio'] >= self._cant_filtros_precio:
                self._protocolo_resultados_cliente.enviar_fin_resultados_filtro_precio()
        else:
            logging.error('acción: recibir_resultado_vuelo | resultado: error')
        
        
        

  
    def enviar_resultado_vuelos_rapidos(self, resultado: ResultadoVuelosRapidos):
        return self._enviar_resultado(resultado, IDENTIFICADOR_RESULTADO_RAPIDOS)

    def enviar_resultado_filtro_distancia(self, resultado: ResultadoFiltroDistancia):
        return self._enviar_resultado(resultado, IDENTIFICADOR_RESULTADO_DISTANCIA)
        
    def enviar_resultado_filtro_escalas(self, resultado: ResultadoFiltroEscalas):
        return self._enviar_resultado(resultado, IDENTIFICADOR_RESULTADO_ESCALAS)

    def enviar_resultado_filtro_precio(self, resultado: ResultadoEstadisticaPrecios):
        return self._enviar_resultado(resultado, IDENTIFICADOR_RESULTADO_PRECIO)

    def _enviar_resultado(self, resultado, identificador_resultado):
        bytes_identificador = identificador_resultado.encode(STRING_ENCODING)
        tamanio, bytes_resultado = resultado.serializar()
        mensaje = bytes_identificador + bytes_resultado
        self._colas.enviar_mensaje(self._nombre_cola, mensaje)

        
    def enviar_fin_resultados_rapidos(self):
        self._colas.enviar_mensaje(self._nombre_cola, IDENTIFICADOR_FIN_RAPIDOS.encode(STRING_ENCODING))

    def enviar_fin_resultados_distancia(self):
        self._colas.enviar_mensaje(self._nombre_cola, IDENTIFICADOR_FIN_DISTANCIA.encode(STRING_ENCODING))

    def enviar_fin_resultados_escalas(self):
        self._colas.enviar_mensaje(self._nombre_cola, IDENTIFICADOR_FIN_ESCALAS.encode(STRING_ENCODING))
    
    def enviar_fin_resultados_filtro_precio(self):
        self._colas.enviar_mensaje(self._nombre_cola, IDENTIFICADOR_FIN_PRECIO.encode(STRING_ENCODING))


   
       
