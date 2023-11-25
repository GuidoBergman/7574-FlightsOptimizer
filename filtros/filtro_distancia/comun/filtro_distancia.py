from struct import unpack, pack, calcsize
import logging
import signal
from geopy.distance import geodesic
from manejador_colas import ManejadorColas
from modelo.estado import Estado
from modelo.Vuelo import Vuelo
from multiprocessing import Process


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
        self.aeropuertos_procesados += 1
        if (self.aeropuertos_procesados % 100) == 1:
            logging.info(f"Procesando aeropuerto: {self.aeropuertos_procesados}")        
        aero_nuevo = {}
        if (id_cliente in self.aeropuertos):
            aero_nuevo = self.aeropuertos[id_cliente]        
        for aeropuerto in aeropuertos_nuevos:
            aero_nuevo[aeropuerto.id] = aeropuerto
        self.aeropuertos[id_cliente] = aero_nuevo

        
    def procesar_finaeropuerto(self, id_cliente):        
        logging.info(f'Fin de Aeropuertos Cliente: {id_cliente}')
    
    def procesar_vuelo(self, id_cliente, vuelos):        
        self.vuelos_procesados += 1;
        if (self.vuelos_procesados % 2) == 1:
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

    def procesar_finvuelo(self, id_cliente):        
        logging.info(f'Fin de vuelos {id_cliente}')
        self._protocoloResultado.enviar_fin_resultados_distancia(id_cliente)
        del self.aeropuertos[id_cliente]        

    def calcular_distancia(self,aeropuerto1, aeropuerto2):
        coordenadas_aeropuerto1 = (aeropuerto1.latitud, aeropuerto1.longitud)
        coordenadas_aeropuerto2 = (aeropuerto2.latitud, aeropuerto2.longitud)
        distancia = geodesic(coordenadas_aeropuerto1, coordenadas_aeropuerto2).kilometers
        return int(distancia)
    
    def run(self):
        try:
          logging.info(f'Iniciando Filtro Distancia')
          self._handle_protocolo_heartbeat = Process(target=self._protocolo_heartbeat.enviar_heartbeats)  
          self._handle_protocolo_heartbeat.start()
          self._protocolo.iniciar(self.procesar_vuelo, self.procesar_finvuelo, self.procesar_aeropuerto, self.procesar_finaeropuerto)
        except Exception as e:
            logging.error(f'Ocurrió una excepción: {e}')
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
        
