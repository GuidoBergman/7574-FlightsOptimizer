import logging
import signal
from geopy.distance import geodesic
from manejador_colas import ManejadorColas
from modelo.estado import Estado
from modelo.Vuelo import Vuelo



from protocolofiltrodistancia import ProtocoloFiltroDistancia
from protocolo_resultados_servidor import ProtocoloResultadosServidor
from modelo.Aeropuerto import Aeropuerto
from modelo.ResultadoFiltroDistancia import ResultadoFiltroDistancia

class FiltroDistancia:
    def __init__(self, id):
       signal.signal(signal.SIGTERM, self.sigterm_handler)
       self._protocolo = ProtocoloFiltroDistancia()
       self._protocoloResultado = ProtocoloResultadosServidor()
       self.aeropuertos = {}
       self.corriendo = True
       self.vuelos_procesados = 0
       self.aeropuertos_procesados = 0
       self._id = id
       
        

    def procesar_aeropuerto(self, aeropuerto: Aeropuerto):
        self.aeropuertos_procesados += 1
        if (self.aeropuertos_procesados % 100) == 1:
            logging.info(f"Procesando aeropuerto: {self.aeropuertos_procesados}")
        
        logging.debug(f'Agregando el aeropuerto { aeropuerto.id }')
        self.aeropuertos[aeropuerto.id] = aeropuerto
        
    def calcular_distancia(self,aeropuerto1, aeropuerto2):
        coordenadas_aeropuerto1 = (aeropuerto1.latitud, aeropuerto1.longitud)
        coordenadas_aeropuerto2 = (aeropuerto2.latitud, aeropuerto2.longitud)
        distancia = geodesic(coordenadas_aeropuerto1, coordenadas_aeropuerto2).kilometers
        return int(distancia)
    
    def procesar_vuelo(self, vuelo: Vuelo):
        
        self.vuelos_procesados += 1;
        if (self.vuelos_procesados % 3000) == 1:
            logging.info(f'Procesando Vuelo: {self.vuelos_procesados}')
        logging.debug(f'Procesando vuelo{ vuelo.id_vuelo } distancia { vuelo.distancia } origen {vuelo.origen} destino {vuelo.destino}')
        try:
            ae_origen = self.aeropuertos[vuelo.origen]
            ae_destino = self.aeropuertos[vuelo.destino]
            logging.debug(f'Aeropuerto origen latitud { ae_origen.latitud } longitud { ae_origen.longitud }')
            logging.debug(f'Aeropuerto destino latitud { ae_destino.latitud } longitud { ae_destino.longitud }')
        
            distancia_directa = self.calcular_distancia(ae_origen, ae_destino)
            logging.debug(f'Distancia directa: { distancia_directa }')
        
            if (distancia_directa * 4 < vuelo.distancia):
                logging.debug(f'Enviando resultado { vuelo.id_vuelo } distancia { vuelo.distancia } distancia directa {distancia_directa}')
                resDistancia = ResultadoFiltroDistancia(vuelo.id_vuelo, vuelo.origen + '-' + vuelo.destino, vuelo.distancia)
                self._protocoloResultado.enviar_resultado_filtro_distancia(resDistancia)
        except KeyError as e:
            logging.error(f'AEROPUERTO NO ENCONTRADO')

    def procesar_finvuelo(self):        
        logging.info(f'Fin de vuelos')
        self._protocoloResultado.enviar_fin_resultados_distancia()
        self.aeropuertos = {}
        self._protocolo.parar_vuelos()
        
    def procesar_finaeropuerto(self):        
        logging.info(f'Fin de Aeropuertos')

    def run(self):
          logging.info(f'Iniciando Filtro Distancia')  
          self._protocolo.iniciar(self.procesar_vuelo, self.procesar_finvuelo, self.procesar_aeropuerto, self.procesar_finaeropuerto)
          

    def sigterm_handler(self, _signo, _stack_frame):
        logging.info('SIGTERM recibida')
        self._protocolo.cerrar()
        self._protocoloResultado.cerrar()
        
