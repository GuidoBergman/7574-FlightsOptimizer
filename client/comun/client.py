import logging
import signal
from modelo.Aeropuerto import Aeropuerto
from socket_comun import SocketComun, STATUS_ERR, STATUS_OK
from protocolo_cliente import ProtocoloCliente
from protocolo_resultados_cliente import (ProtocoloResultadosCliente, 
            IDENTIFICADOR_FIN_RAPIDOS, IDENTIFICADOR_FIN_DISTANCIA, IDENTIFICADOR_FIN_ESCALAS,
            IDENTIFICADOR_FIN_PRECIO)
from multiprocessing import Process
from comun.enviador_vuelos import EnviadorVuelos

CANT_TIPOS_RESULTADO = 4

class Client:
    def __init__(self, host, port):
        # Initialize server socket
        server_socket = SocketComun()
        server_socket.connect(host, port)
        self._protocolo = ProtocoloCliente(server_socket)
        self._protocolo_resultados = ProtocoloResultadosCliente(server_socket)
        signal.signal(signal.SIGTERM, self.sigterm_handler)    


    def _enviar_aeropuertos(self, nombre_archivo: str):
        with open(nombre_archivo, 'r', encoding='utf-8') as archivo:
            next(archivo)  # Saltar la primera línea con encabezados
            for linea in archivo:
                campos = linea.strip().split(';')
                if len(campos) >= 7:
                    codigo = campos[0]  # Airport Code
                    latitud = float(campos[5])  # Latitude
                    longitud = float(campos[6])  # Longitude

                    aeropuerto = Aeropuerto(codigo, latitud, longitud)
                    self._protocolo.enviar_aeropuerto(aeropuerto)

        self._protocolo.enviar_fin_aeropuertos()

    def _recibir_resultados(self):
        fines_recibidos = set()
        while len(fines_recibidos) < CANT_TIPOS_RESULTADO:
            logging.info('action: recibir_resultado | estado: esperando')
            estado, resultado = self._protocolo_resultados.recibir_resultado()
            if estado == STATUS_ERR:
                logging.info('action: recibir_resultado | resultado: error')
                break
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
                logging.debug(f'action: recibir_resultado | resultado: OK  | {resultado.convertir_a_str()}')

        logging.info(f'action: recibir_resultado | resultado: se recibieron todos los resultados')
        


    def sigterm_handler(self, _signo, _stack_frame):
        logging.info('acción: sigterm_recibida')
        self._protocolo.cerrar()
        if self._handler_proceso:
            self._handler_proceso.terminate()
            self._handler_proceso.join()
        

    def run(self):
        logging.info("Iniciando cliente")
        try:
            self._enviar_aeropuertos('airports-codepublic.csv')
        except (ConnectionResetError, BrokenPipeError, OSError):
            return

        enviador_vuelos = EnviadorVuelos(self._protocolo)
        self._handler_proceso = Process(target=enviador_vuelos.enviar_vuelos, args=(('itineraries_short.csv'),))
        self._handler_proceso.start()

        try:
            self._recibir_resultados()
        except (ConnectionResetError, BrokenPipeError, OSError):
            return

        self._protocolo.cerrar()
        self._handler_proceso.join()


                


            
    
        
        
        
           
