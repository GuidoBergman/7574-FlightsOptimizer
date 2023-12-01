import logging
import os
from math import log
import signal
import struct
from comun.lista_precios import ListaPrecios
from manejador_colas import ManejadorColas
from modelo.estado import Estado
from modelo.Vuelo import Vuelo
from modelo.ResultadoEstadisticasPrecios import ResultadoEstadisticaPrecios
from protocolofiltroprecio import ProtocoloFiltroPrecio
from protocolo_resultados_servidor import ProtocoloResultadosServidor
from multiprocessing import Process
from protocolo_enviar_heartbeat import ProtocoloEnviarHeartbeat, IDENTIFICADOR_FILTRO_PRECIO
from socket_comun_udp import SocketComunUDP

REGISTROS_EN_MEMORIA = 1000

class FiltroPrecios:
    
    def __init__(self, id, cant_watchdogs, periodo_heartbeat, host_watchdog, port_watchdog):
       self._protocolo = ProtocoloFiltroPrecio()
       self._id = id
       self.inicialar()   
       self.corriendo = True

       signal.signal(signal.SIGTERM, self.sigterm_handler)

       socket = SocketComunUDP()
       self._protocolo_heartbeat = ProtocoloEnviarHeartbeat(socket, host_watchdog, port_watchdog, cant_watchdogs,
        IDENTIFICADOR_FILTRO_PRECIO, periodo_heartbeat, id)
       
    def inicialar(self):
       self.precios = {}    
       self.vuelos_procesados = 0
       self.resultados_enviados = 0
        
    def procesar_vuelos(self, id_cliente, vuelos):        
        self.vuelos_procesados += 1;
        if (self.vuelos_procesados % 10) == 1:
            logging.debug(f'Procesando Vuelo: {self.vuelos_procesados}')  
        
        if (id_cliente in self.aeropuertos):
            listaprecio = self.precios[id_cliente]
        else:
            listaprecio = ListaPrecios(id_cliente)            
        contenido_a_persisitir = listaprecio.agregar_vuelos(vuelos)
        self.precios[id_cliente] = listaprecio
        return contenido_a_persisitir
    
            
    def procesar_flush(self, id_cliente):        
        logging.info(f'FLUSH Cliente: {id_cliente}')
        return None
        
    
    def procesar_finvuelo(self, id_cliente):        
        logging.info(f'Calculo el promedio y lo envia')        
        self._protocolo.enviar_promedio(id_cliente, self.precios[id_cliente].promedio, self.precios[id_cliente].cantidad, self._id)
        return "promedio_enviado"

    def _recuperar_promedios(self):
        logging.info("Recuperando promedios..")
        for id_cliente, linea in self._protocolo.recuperar_siguiente_linea():
            logging.debug(f'Recupere la linea del cliente {id_cliente}')
                # Proceso aeropuertos        
            if linea[0] == ("V"):
                if (id_cliente in self.precios):
                    lista_precio = self.precios[id_cliente]
                else:
                    lista_precio = ListaPrecios(id_cliente)            
                lista_precio.recuperar_promedios(linea)
                self.precios[id_cliente] = lista_precio
        

    def procesar_promediogeneral(self, id_cliente, promedio):
        logging.info(f"Recibe el promedio {promedio} del cliente {id_cliente}")
        self._protocoloResultado = ProtocoloResultadosServidor()
        resultados = self.precios[id_cliente].get_resultados()
        self._protocoloResultado.enviar_resultado_filtro_precio(resultados, id_cliente)
        self._protocoloResultado.enviar_fin_resultados_filtro_precio(id_cliente, self._id)
        self.borrar_archivos(id_cliente)

        return None

    def run(self):
        try:
          self._recuperar_promedios()
          self._handle_protocolo_heartbeat = Process(target=self._protocolo_heartbeat.enviar_heartbeats)  
          self._handle_protocolo_heartbeat.start()
          self._protocolo.iniciar(self.procesar_vuelos, self.procesar_finvuelo, self.procesar_promediogeneral, self.procesar_flush, self._id)
        except Exception as e:
            logging.error(f'Ocurrió una excepción: {e}')
            self.cerrar()
          

    def sigterm_handler(self, _signo, _stack_frame):
        logging.error('SIGTERM recibida')
        self.cerrar()
        

    def cerrar(self):
        logging.error('Cerrando recursos')
        self._protocolo.cerrar()
        if hasattr(self, '_protocoloResultado'):
            self._protocoloResultado.cerrar()

        if hasattr(self, '_protocolo_heartbeat'):
            self._protocolo_heartbeat.cerrar()

        if hasattr(self, '_handle_protocolo_heartbeat'):
            if self._handle_protocolo_heartbeat.is_alive():
                self._handle_protocolo_heartbeat.terminate()
            self._handle_protocolo_heartbeat.join()
