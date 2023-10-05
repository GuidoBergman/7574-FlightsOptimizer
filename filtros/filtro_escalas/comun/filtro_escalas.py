import logging
import signal
from modelo.Vuelo import Vuelo
from manejador_colas import ManejadorColas
from modelo.estado import Estado
from protocolofiltroescalas import ProtocoloFiltroEscalas
from protocoloresultados import ProtocoloResultado
from protocolovelocidad import ProtocoloFiltroVelocidad


class FiltroEscalas:
    def __init__(self):
       signal.signal(signal.SIGTERM, self.sigterm_handler)
       
       self._protocolo = ProtocoloFiltroEscalas()
       self._protocoloResultado = ProtocoloResultado()
       self._protocoloVelocidad = ProtocoloFiltroVelocidad()
       self.vuelos_con_tres_escalas = []
       self.corriendo = True
       
        
    def sigterm_handler(self, _signo, _stack_frame):
        self._protocolo.parar()
        logging.info('action: sigterm_received')

        
    def procesar_vuelo(self, vuelo: Vuelo):
        if len(vuelo.escalas) > 3:
            self._protocoloResultado.enviar_resultado_filtro_escalas(vuelo)
            self._protocoloVelocidad.enviar_vuelo(vuelo)

    def procesar_finvuelo(self, vuelo: Vuelo):
        self._protocoloVelocidad.enviar_fin_vuelos()

            
        
    def run(self):
          self._protocolo.iniciar(self.procesar_vuelo, self.procesar_finvuelo)
          #self._protocoloResultado.iniciar()
          #self._protocoloVelocidad.iniciar()
          while self._protocolo.corriendo:
              a = 1
          return