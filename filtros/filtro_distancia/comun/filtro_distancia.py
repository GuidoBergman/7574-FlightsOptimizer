import logging
import signal

from manejador_colas import ManejadorColas
from modelo.estado import Estado
from modelo.Vuelo import Vuelo



from protocolofiltrodistancia import ProtocoloFiltroDistancia
from protocolo_resultados_servidor import ProtocoloResultadosServidor
from protocolos.modelo.Aeropuerto import Aeropuerto


class FiltroDistancia:
    def __init__(self):
       signal.signal(signal.SIGTERM, self.sigterm_handler)
       self._protocolo = ProtocoloFiltroDistancia()
       self._protocoloResultado = ProtocoloResultadosServidor()
       self.vuelos_con_tres_escalas = []
       self.corriendo = True
       
        
    def sigterm_handler(self, _signo, _stack_frame):
        self._protocolo.parar()
        logging.info('action: sigterm_received')


    def procesar_aeropuerto(self, aeropuerto: Aeropuerto):
        logging.error(f'Procesando el aeropuerto { aeropuerto.id }')
        
    def procesar_vuelo(self, vuelo: Vuelo):
        logging.error(f'Procesando el vuelo{ vuelo.id_vuelo } distancia { vuelo.distancia }')

    def procesar_finvuelo(self):        
        logging.error(f'FIN DE VUELOS')
        

    def run(self):
          self._protocolo.iniciar(self.procesar_vuelo, self.procesar_finvuelo)
          self._protocoloResultado.iniciar()
          self._protocoloVelocidad.iniciar()
          while self._protocolo.corriendo:
              a = 1
          return