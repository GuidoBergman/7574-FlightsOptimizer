import logging
import signal
from manejador_colas import ManejadorColas
from geopy.distance import great_circle
from modelo.Aeropuerto import Aeropuerto
from modelo.estado import Estado
from protocolofiltrodistancias import ProtocoloFiltroDistancia
from protocoloresultados import ProtocoloResultado

class FiltroDistancia:
    def __init__(self, port, listen_backlog):
       self._colas = ManejadorColas('rabbitmq')
       signal.signal(signal.SIGTERM, self.sigterm_handler)
       self._protocoloResultado = ProtocoloResultado()
       self._protocolo = ProtocoloFiltroDistancia()
       self.aeropuertos = []
        
        
    def agregar_aeropuerto(self, aeropuerto: Aeropuerto):
        self.aeropuertos.append(aeropuerto)


    def calcular_distancia(self, origen: str, destino: str) -> float:
        # Buscar los objetos Aeropuerto correspondientes a los códigos de origen y destino
        aeropuerto_origen = next((a for a in self.aeropuertos if a.codigo == origen), None)
        aeropuerto_destino = next((a for a in self.aeropuertos if a.codigo == destino), None)

        if aeropuerto_origen and aeropuerto_destino:
            # Calcular la distancia entre los dos aeropuertos utilizando great_circle
            coordenadas_origen = (aeropuerto_origen.latitud, aeropuerto_origen.longitud)
            coordenadas_destino = (aeropuerto_destino.latitud, aeropuerto_destino.longitud)
            distancia = great_circle(coordenadas_origen, coordenadas_destino).kilometers
            return distancia

        return 0.0

    def procesar_vuelo(self, vuelo: Vuelo):
        trayecto = vuelo.trayecto
        origen, destino = trayecto.split('-')
        distancia_total = vuelo.distancia_total

        # Calcular la distancia directa entre origen y destino
        distancia_directa = self.calcular_distancia(origen, destino)

        if distancia_directa > 0 and distancia_total > 4 * distancia_directa:
            self._protocoloResultado.enviar_resultado_filtro_distancia()


    
    def enviar(vuelo):
        print(vuelo)

    def sigterm_handler(self, _signo, _stack_frame):
        logging.info('action: sigterm_received')


    def run(self):
          self._colas.crear_cola('cola')
          self._colas.consumir_mensajes('cola')
          
          while self.corriendo:
            aeropuerto, estado = self._protocolo.recibir_aeropuerto()
            if estado == Estado.OK:
                self.agregar_aeropuerto(aeropuerto)

            vuelo, estado = self._protocolo.recibir_vuelo()
            if estado == Estado.OK:
                self.procesar_vuelo(vuelo)
            else:
                break



            
    
        
        
        
           
