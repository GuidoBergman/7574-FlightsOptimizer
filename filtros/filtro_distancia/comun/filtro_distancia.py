from functools import lru_cache
import os
from struct import unpack, pack, calcsize
import logging
import signal
from comun.lista_aeropuertos import ListaAeropuertos
from geopy.distance import geodesic
from manejador_colas import ManejadorColas
from modelo.estado import Estado
from modelo.Vuelo import Vuelo
from multiprocessing import Process
import sys
import traceback


from protocolofiltrodistancia import ProtocoloFiltroDistancia
from protocolo_resultados_servidor import ProtocoloResultadosServidor
from modelo.Aeropuerto import Aeropuerto
from modelo.ResultadoFiltroDistancia import ResultadoFiltroDistancia
from protocolo_enviar_heartbeat import ProtocoloEnviarHeartbeat, IDENTIFICADOR_FILTRO_DISTANCIA
from socket_comun_udp import SocketComunUDP

class FiltroDistancia:
    def __init__(self, id, cant_watchdogs, periodo_heartbeat, host_watchdog, port_watchdog):
       signal.signal(signal.SIGTERM, self.sigterm_handler)
       self._protocolo = ProtocoloFiltroDistancia()
       self._protocoloResultado = ProtocoloResultadosServidor()
       self.aeropuertos = {}
       self.corriendo = True
       self.vuelos_procesados = 0
       self.aeropuertos_procesados = 0
       self._id = id
       socket = SocketComunUDP()
       self._protocolo_heartbeat = ProtocoloEnviarHeartbeat(socket, host_watchdog, port_watchdog, cant_watchdogs,
        IDENTIFICADOR_FILTRO_DISTANCIA, periodo_heartbeat, id)

    def procesar_aeropuerto(self, id_cliente, aeropuertos_nuevos):

        # LOGGING
        self.aeropuertos_procesados += 1
        if (self.aeropuertos_procesados % 100) == 1:
            logging.info(f"Procesando aeropuerto: {self.aeropuertos_procesados}")        
            
        # Proceso aeropuertos        
        if (id_cliente in self.aeropuertos):
            aero_nuevo = self.aeropuertos[id_cliente]
        else:
            aero_nuevo = ListaAeropuertos(id_cliente)            
        aero_nuevo.agregar_aeropuertos(aeropuertos_nuevos)
        self.aeropuertos[id_cliente] = aero_nuevo
        return None

    def procesar_flush(self, id_cliente):        
        logging.info(f'FLUSH Cliente: {id_cliente}')
        self.aeropuertos[id_cliente].borrar_archivos()
        return None
        
    def procesar_finaeropuerto(self, id_cliente):        
        logging.info(f'Fin de Aeropuertos Cliente: {id_cliente}')

        return None
    
    def procesar_vuelo(self, id_cliente, vuelos):        
        self.vuelos_procesados += 1;
        if (self.vuelos_procesados % 300) == 1:
            logging.info(f'Procesando Vuelo: {self.vuelos_procesados}')
            
        aeropuertos_cliente = self.aeropuertos[id_cliente]
        resultados = []
        for vuelo in vuelos:
            logging.debug(f'Procesando vuelo{ vuelo.id_vuelo } distancia { vuelo.distancia } origen {vuelo.origen} destino {vuelo.destino}')
            try:
                ae_origen = aeropuertos_cliente[vuelo.origen]
                ae_destino = aeropuertos_cliente[vuelo.destino]
                logging.debug(f'Aeropuerto origen latitud { ae_origen.latitud } longitud { ae_origen.longitud }')
                logging.debug(f'Aeropuerto destino latitud { ae_destino.latitud } longitud { ae_destino.longitud }')
        
                distancia_directa = self.calcular_distancia(ae_origen, ae_destino)
                logging.debug(f'Distancia directa: { distancia_directa }')
        
                if (distancia_directa * 4 < vuelo.distancia):
                    logging.debug(f'Enviando resultado { vuelo.id_vuelo } distancia { vuelo.distancia } distancia directa {distancia_directa}')
                    resDistancia = ResultadoFiltroDistancia(vuelo.id_vuelo, vuelo.origen + '-' + vuelo.destino, vuelo.distancia)
                    resultados.append(resDistancia)
            except KeyError as e:
                logging.error(f'AEROPUERTO NO ENCONTRADO')
        self._protocoloResultado.enviar_resultado_filtro_distancia(resultados, id_cliente)

        return None

    def procesar_finvuelo(self, id_cliente):        
        logging.info(f'Fin de vuelos distancia {id_cliente}')
        self._protocoloResultado.enviar_fin_resultados_distancia(id_cliente)
        self.aeropuertos[id_cliente].borrar_archivos()
        del self.aeropuertos[id_cliente]        

        return None
    
    
    @lru_cache(maxsize=None)
    def distancia_entrecoordenadas(self,coordenadas_aeropuerto1, coordenadas_aeropuerto2):
        distancia = geodesic(coordenadas_aeropuerto1, coordenadas_aeropuerto2).kilometers
        return int(distancia)
        
    def calcular_distancia(self,aeropuerto1, aeropuerto2):
        coordenadas_aeropuerto1 = (aeropuerto1.latitud, aeropuerto1.longitud)
        coordenadas_aeropuerto2 = (aeropuerto2.latitud, aeropuerto2.longitud)
        return self.distancia_entrecoordenadas(coordenadas_aeropuerto1, coordenadas_aeropuerto2)

    def recuperar_aeropuertos(self):
        archivos_definitivos = [archivo for archivo in os.listdir() if archivo.startswith("aero_def_")]
        for archivo in archivos_definitivos:
            id_cliente = archivo.split("_")[2].split(".")[0]
            logging.info(f"Recuperando aeropuertos de {id_cliente}")
            lista_aeropuertos = ListaAeropuertos(id_cliente)
            lista_aeropuertos.recuperar_aeropuertos()
            self.aeropuertos[id_cliente] = lista_aeropuertos


        
    def run(self):
        try:
          logging.info(f'Iniciando Filtro Distancia')
          self._handle_protocolo_heartbeat = Process(target=self._protocolo_heartbeat.enviar_heartbeats)  
          self._handle_protocolo_heartbeat.start()
          self.recuperar_aeropuertos()
          self._protocolo.iniciar(self.procesar_vuelo, self.procesar_finvuelo, self.procesar_aeropuerto, self.procesar_finaeropuerto, self.procesar_flush, self._id)
        except Exception as e:
            logging.error(f'Ocurrió una excepción: {e}')
            exc = sys.exception()
            traceback.print_tb(exc.__traceback__, limit=1, file=sys.stdout)          
            traceback.print_exception(exc, limit=2, file=sys.stdout)
            self.cerrar()
        

    def sigterm_handler(self, _signo, _stack_frame):
        logging.error('SIGTERM recibida')
        self.cerrar()
        

    def cerrar(self):
        logging.error('Cerrando recursos')
        self._protocolo.cerrar()
        self._protocoloResultado.cerrar()
        if hasattr(self, '_protocolo_heartbeat'):
            self._protocolo_heartbeat.cerrar()

        if hasattr(self, '_handle_protocolo_heartbeat'):
            if self._handle_protocolo_heartbeat.is_alive():
                self._handle_protocolo_heartbeat.terminate()
            self._handle_protocolo_heartbeat.join()
        
