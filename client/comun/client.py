import logging
import signal
import os
from modelo.Aeropuerto import Aeropuerto
from modelo.ResultadoFiltroDistancia import ResultadoFiltroDistancia
from modelo.ResultadoFiltroEscalas import ResultadoFiltroEscalas
from modelo.ResultadoVuelosRapidos import ResultadoVuelosRapidos
from modelo.ResultadoEstadisticasPrecios import ResultadoEstadisticaPrecios

from socket_comun import SocketComun, STATUS_ERR, STATUS_OK
from protocolo_cliente import ProtocoloCliente
from protocolo_resultados_cliente import (ProtocoloResultadosCliente, 
            IDENTIFICADOR_FIN_RAPIDOS, IDENTIFICADOR_FIN_DISTANCIA, IDENTIFICADOR_FIN_ESCALAS,
            IDENTIFICADOR_FIN_PRECIO)
from multiprocessing import Process
from comun.enviador_vuelos import EnviadorVuelos

CANT_TIPOS_RESULTADO = 4
CHUNK_AEROPUERTOS = 300

class Client:
    def __init__(self, host, port, archivo_aeropuertos, archivo_vuelos ):
        # Initialize server socket
        server_socket = SocketComun()
        server_socket.connect(host, port)
        self.archivo_aeropuertos = archivo_aeropuertos
        self.archivo_vuelos = archivo_vuelos
        self._protocolo = ProtocoloCliente(server_socket)
        self._protocolo_resultados = ProtocoloResultadosCliente(server_socket)
        signal.signal(signal.SIGTERM, self.sigterm_handler)    


    def _enviar_aeropuertos(self, nombre_archivo: str):
        aeropuertos = []
        with open('/data/' + nombre_archivo, 'r', encoding='utf-8') as archivo:
            next(archivo)  # Saltar la primera línea con encabezados
            for linea in archivo:
                campos = linea.strip().split(';')
                if len(campos) >= 7:
                    codigo = campos[0]  # Airport Code
                    latitud = float(campos[5])  # Latitude
                    longitud = float(campos[6])  # Longitude

                    aeropuerto = Aeropuerto(codigo, latitud, longitud)
                    aeropuertos.append(aeropuerto)
                    if (len(aeropuertos) > CHUNK_AEROPUERTOS):
                        self._protocolo.enviar_aeropuertos(aeropuertos)
                        aeropuertos = []
        
        if (len(aeropuertos) > 0):
            self._protocolo.enviar_aeropuertos(aeropuertos)
            aeropuertos = []
        self._protocolo.enviar_fin_aeropuertos()

    def _recibir_resultados(self):
        fines_recibidos = set()
        resultados_recibidos = 0
        archivo_ResultadoFiltroDistancia = '/resultados/ResultadoFiltroDistancia.txt'
        archivo_ResultadoFiltroEscalas = '/resultados/ResultadoFiltroEscalas.txt'    
        archivo_ResultadoVuelosRapidos = '/resultados/ResultadoVuelosRapidos.txt'    
        archivo_ResultadoEstadisticaPrecios = '/resultados/ResultadoEstadisticaPrecios.txt'

        if os.path.exists(archivo_ResultadoFiltroDistancia):
            os.remove(archivo_ResultadoFiltroDistancia)      
        if os.path.exists(archivo_ResultadoFiltroEscalas):
            os.remove(archivo_ResultadoFiltroEscalas)
        if os.path.exists(archivo_ResultadoVuelosRapidos):
            os.remove(archivo_ResultadoVuelosRapidos)
        if os.path.exists(archivo_ResultadoEstadisticaPrecios):
            os.remove(archivo_ResultadoEstadisticaPrecios)

        with open(archivo_ResultadoFiltroDistancia, 'w') as aResultadoFiltroDistancia:
            with open(archivo_ResultadoFiltroEscalas, 'w') as aResultadoFiltroEscalas:
                with open(archivo_ResultadoVuelosRapidos, 'w') as aResultadoVuelosRapidos:
                    with open(archivo_ResultadoEstadisticaPrecios, 'w') as aResultadoEstadisticaPrecios:
                        while len(fines_recibidos) < CANT_TIPOS_RESULTADO:
                            logging.debug('action: recibir_resultado | estado: esperando')
                            estado, resultados = self._protocolo_resultados.recibir_resultado()
                            if estado == STATUS_ERR:
                                logging.error('action: recibir_resultado | resultado: error')
                                return
                            elif estado == IDENTIFICADOR_FIN_RAPIDOS:
                                fines_recibidos.add(estado)
                                logging.info('action: recibir_resultado | resultado: se recibieron todos los resultados de los vuelos rápidos')
                            elif estado == IDENTIFICADOR_FIN_DISTANCIA:
                                fines_recibidos.add(estado)
                                logging.info('action: recibir_resultado | resultado: se recibieron todos los resultados de los vuelos con distancia larga')
                            elif estado == IDENTIFICADOR_FIN_ESCALAS:
                                fines_recibidos.add(estado)
                                logging.info('action: recibir_resultado | resultado: se recibieron todos los resultados de los vuelos con más escalas')
                            elif estado == IDENTIFICADOR_FIN_PRECIO:
                                fines_recibidos.add(estado)
                                logging.info('action: recibir_resultado | resultado: se recibieron todos los resultados de las estadisticas de los precios costosos')
                            else:
                                for resultado in resultados:
                                    resultados_recibidos +=1;
                                    if (resultados_recibidos % 1000) == 1:
                                        logging.info(f"Resultados recibidos : {resultados_recibidos}")
                                    if type(resultado) is ResultadoFiltroDistancia: 
                                        aResultadoFiltroDistancia.write(resultado.convertir_a_str() + '\n'),
                                    if type(resultado) is ResultadoFiltroEscalas: 
                                        aResultadoFiltroEscalas.write(resultado.convertir_a_str() + '\n'),
                                    if type(resultado) is ResultadoVuelosRapidos: 
                                        aResultadoVuelosRapidos.write(resultado.convertir_a_str() + '\n' ),
                                    if type(resultado) is ResultadoEstadisticaPrecios: 
                                        aResultadoEstadisticaPrecios.write(resultado.convertir_a_str() + '\n')
                                    
        logging.info(f'action: recibir_resultado | resultado: se recibieron todos los resultados {resultados_recibidos}')
        


    def sigterm_handler(self, _signo, _stack_frame):
        logging.info('acción: sigterm_recibida')
        self._protocolo.cerrar()
        if self._handler_proceso:
            self._handler_proceso.terminate()
            self._handler_proceso.join()
        

    def run(self):
        logging.info("Iniciando cliente")
        
        logging.info("Enviando Aeropuertos")
        try:
            self._enviar_aeropuertos(self.archivo_aeropuertos)
        except (ConnectionResetError, BrokenPipeError, OSError) as e:
            error_type = type(e).__name__  # Obtiene el nombre del tipo de excepción
            error_message = str(e)          # Obtiene el mensaje de error
            logging.error(f"Error conectando: {error_type} - {error_message}")
            return


        logging.info("Enviando Vuelos")
        enviador_vuelos = EnviadorVuelos(self._protocolo)
        self._handler_proceso = Process(target=enviador_vuelos.enviar_vuelos, args=(('/data/' + self.archivo_vuelos),))
        self._handler_proceso.start()

        try:
            self._recibir_resultados()
        except (ConnectionResetError, BrokenPipeError, OSError):
            return

        self._protocolo.cerrar()
        self._handler_proceso.join()


                


            
    
        
        
        
           
