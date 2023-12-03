from struct import unpack, pack, calcsize
from protocolo_resultados_cliente import ProtocoloResultadosCliente
from manejador_colas import ManejadorColas
import logging
import signal
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
from recuperador import Recuperador


COLA_RESULTADOS = 'cola_resultados'
STRING_ENCODING = 'utf-8'
CANT_TIPOS_RESULTADO = 4

FORMATOCABECERA_RESULTADOS = '!cH'
FORMATO_FIN_RESULTADOS = '!cH'

class ProtocoloResultadosServidor():

    def __init__(self, id_cliente=None):
       self._recuperador = Recuperador()
       self._colas = ManejadorColas()
       self._corriendo = False
       self.id_cliente = id_cliente
       if self.id_cliente is None:
            self.id_cliente = "" 
       self._nombre_cola = COLA_RESULTADOS + self.id_cliente
       self.resultados_recibidos = 0
       self.filtros_completos = 0

    # Escuchar los resultados en la cola de resultados del servidor y mandarselos al cliente   
    def iniciar(self, socket_cliente, cant_filtros_escalas,
    cant_filtros_distancia, cant_filtros_velocidad, cant_filtros_precio):      
        self._protocolo_resultados_cliente = ProtocoloResultadosCliente(socket_cliente)
        signal.signal(signal.SIGTERM, self._sigterm_handler)
        self._cant_filtros_escalas = cant_filtros_escalas
        self._cant_filtros_distancia = cant_filtros_distancia
        self._cant_filtros_velocidad = cant_filtros_velocidad
        self._cant_filtros_precio = cant_filtros_precio

        self._cant_fines_resultados = {
            'escalas': 0, 'distancia': 0, 'velocidad': 0, 'precio': 0
        }

        self._colas.crear_cola(self._nombre_cola)
        self._colas.consumir_mensajes(self._nombre_cola, self._callback_function)
        self._colas.consumir()
        
    def finalizo_resultados(self, identificador):
        self.filtros_completos += 1
        if (self.filtros_completos >= CANT_TIPOS_RESULTADO):
            self._colas.dejar_de_consumir(self._nombre_cola)
        
    def _callback_function(self, body):
        if self._recuperador.es_duplicado(self.id_cliente, body):
                logging.debug(f'Se recibió un mensaje duplicado: {body}')
                return

        contenido_a_persistir = None
        self.resultados_recibidos += 1;
        if (self.resultados_recibidos % 300) == 1:
            logging.info(f"Resultados recibidos {self.resultados_recibidos}")
        identificador_resultado = body[0:1].decode(STRING_ENCODING)
        if identificador_resultado == IDENTIFICADOR_RESULTADO_RAPIDOS:
            self._protocolo_resultados_cliente.enviar_resultados(body)
        elif identificador_resultado == IDENTIFICADOR_RESULTADO_DISTANCIA:
            self._protocolo_resultados_cliente.enviar_resultados(body)
        elif identificador_resultado == IDENTIFICADOR_RESULTADO_ESCALAS:
            self._protocolo_resultados_cliente.enviar_resultados(body)
        elif identificador_resultado == IDENTIFICADOR_RESULTADO_PRECIO:
            logging.info("Llego Fin de Precios")
            self._protocolo_resultados_cliente.enviar_resultados(body)
        elif identificador_resultado == IDENTIFICADOR_FIN_RAPIDOS:
            logging.info("Llego Fin de Velocidad")
            self._cant_fines_resultados['velocidad'] += 1
            if self._cant_fines_resultados['velocidad'] >= self._cant_filtros_velocidad:
                self.finalizo_resultados(IDENTIFICADOR_FIN_RAPIDOS)
                self._protocolo_resultados_cliente.enviar_fin_resultados_rapidos()
        elif identificador_resultado == IDENTIFICADOR_FIN_DISTANCIA:
            logging.info("Llego Fin de Distancia")
            self._cant_fines_resultados['distancia'] += 1
            if self._cant_fines_resultados['distancia'] >= self._cant_filtros_distancia:
                self.finalizo_resultados(IDENTIFICADOR_FIN_DISTANCIA)
                self._protocolo_resultados_cliente.enviar_fin_resultados_distancia()
        elif identificador_resultado == IDENTIFICADOR_FIN_ESCALAS:
            logging.info("Llego Fin de Escalas")
            self._cant_fines_resultados['escalas'] += 1
            if self._cant_fines_resultados['escalas'] >= self._cant_filtros_escalas:
                self.finalizo_resultados(IDENTIFICADOR_FIN_ESCALAS)
                self._protocolo_resultados_cliente.enviar_fin_resultados_escalas()
        elif identificador_resultado == IDENTIFICADOR_FIN_PRECIO:
            self._cant_fines_resultados['precio'] += 1
            if self._cant_fines_resultados['precio'] >= self._cant_filtros_precio:
                self.finalizo_resultados(IDENTIFICADOR_FIN_PRECIO)
                self._protocolo_resultados_cliente.enviar_fin_resultados_filtro_precio()
        else:
            logging.error('acción: recibir_resultado_vuelo | resultado: error')

        self._recuperador.almacenar(self.id_cliente, body, contenido_a_persistir)
        
        

  
    def enviar_resultado_vuelos_rapidos(self, resultado: ResultadoVuelosRapidos, id_cliente):
        return self._enviar_resultados(resultado, IDENTIFICADOR_RESULTADO_RAPIDOS, id_cliente)

    def enviar_resultado_filtro_distancia(self, resultado: ResultadoFiltroDistancia, id_cliente):
        return self._enviar_resultados(resultado, IDENTIFICADOR_RESULTADO_DISTANCIA, id_cliente)
        
    def enviar_resultado_filtro_escalas(self, resultado: ResultadoFiltroEscalas, id_cliente):
        return self._enviar_resultados(resultado, IDENTIFICADOR_RESULTADO_ESCALAS, id_cliente)

    def enviar_resultado_filtro_precio(self, resultado: ResultadoEstadisticaPrecios, id_cliente):
        return self._enviar_resultados(resultado, IDENTIFICADOR_RESULTADO_PRECIO, id_cliente)

    def _enviar_resultados(self, resultados, identificador_resultado, id_cliente):
        if len(resultados) == 0:
            return
        msg = pack(FORMATOCABECERA_RESULTADOS, identificador_resultado.encode(STRING_ENCODING), len(resultados))
        for resultado in resultados:
            tamanio, bytes_resultado = resultado.serializar()
            msg = msg + bytes_resultado
        logging.debug(f'Enviando resultado a la cola: {self._nombre_cola + id_cliente}')
        self._colas.enviar_mensaje(self._nombre_cola + id_cliente, msg)

        
    def enviar_fin_resultados_rapidos(self, id_cliente, id_filtro):
        self._enviar_fin_resultados(id_cliente, IDENTIFICADOR_FIN_RAPIDOS, id_filtro)

    def enviar_fin_resultados_distancia(self, id_cliente, id_filtro):
        self._enviar_fin_resultados(id_cliente, IDENTIFICADOR_FIN_DISTANCIA, id_filtro)

    def enviar_fin_resultados_escalas(self, id_cliente, id_filtro):
        self._enviar_fin_resultados(id_cliente, IDENTIFICADOR_FIN_ESCALAS, id_filtro)
    
    def enviar_fin_resultados_filtro_precio(self, id_cliente, id_filtro):
       self._enviar_fin_resultados(id_cliente, IDENTIFICADOR_FIN_PRECIO, id_filtro)

    def _enviar_fin_resultados(self, id_cliente, id_fin, id_filtro):
        msg = pack(FORMATO_FIN_RESULTADOS, id_fin.encode(STRING_ENCODING), id_filtro)
        logging.debug(f'Enviando resultado a la cola: {self._nombre_cola + id_cliente}')
        self._colas.enviar_mensaje(self._nombre_cola + id_cliente, msg)

       
    def _sigterm_handler(self, _signo, _stack_frame):
        logging.error('Sigterm recibida (enviador resultados)')
        self.parar()
        self._colas.cerrar()
        self._protocolo_resultados_cliente.cerrar()

    def parar(self):        
        self._colas.dejar_de_consumir(COLA_RESULTADOS)

    
    def cerrar(self):
        self._colas.cerrar()
        self._recuperador.cerrar()