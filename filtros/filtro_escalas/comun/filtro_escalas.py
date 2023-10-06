import logging
import signal

from manejador_colas import ManejadorColas
from modelo.estado import Estado
from protocolofiltroescalas import ProtocoloFiltroEscalas
from protocoloresultados import ProtocoloResultado
from modelo.Vuelo import Vuelo

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
        
        
        logging.error(f'Procesando el vuelo{ vuelo.id_vuelo } escalas { vuelo.escalas }')
        if len(vuelo.escalas.split("||")) >= 3:            
            logging.error(f'Envia como resultado el vuelo { vuelo.id_vuelo }')
            self._protocoloResultado.enviar_resultado_filtro_escalas(vuelo)
            self._protocoloVelocidad.enviar_vuelo(vuelo)

    def procesar_finvuelo(self):        
        logging.error(f'FIN DE VUELOS')
        self._protocoloVelocidad.enviar_fin_vuelos()

    def run(self):
          self._protocolo.iniciar(self.procesar_vuelo, self.procesar_finvuelo)
          self._protocoloResultado.iniciar()
          self._protocoloVelocidad.iniciar()
          while self._protocolo.corriendo:
              a = 1
          return