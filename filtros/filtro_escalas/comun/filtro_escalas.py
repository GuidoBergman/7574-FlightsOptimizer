import logging
import signal
from modelo.Vuelo import Vuelo
from manejador_colas import ManejadorColas
from modelo.estado import Estado
from protocoloescalas import ProtocoloFiltroEscalas
from protocoloresultados import ProtocoloResultado
from protocolovelocidad import ProtocoloFiltroVelocidad


class FiltroEscalas:
    def __init__(self, port, listen_backlog):
       self._colas = ManejadorColas('rabbitmq')
       signal.signal(signal.SIGTERM, self.sigterm_handler)
       
       self._protocoloResultado = ProtocoloResultado()
       self._protocoloVelocidad = ProtocoloFiltroVelocidad()
       self._protocolo = ProtocoloFiltroEscalas()
       self.vuelos_con_tres_escalas = []
       self.corriendo = True
       
        
    def sigterm_handler(self, _signo, _stack_frame):
        logging.info('action: sigterm_received')

        
    def procesar_vuelo(self, vuelo: Vuelo):
        if len(vuelo.escalas) > 3:
            self._protocoloResultado.EnviarResultadoFiltroEscalas(vuelo)
            self._protocoloVelocidad.enviar_vuelo(vuelo)

            
        
    def run(self):
          self._colas.crear_cola('cola')
          self._colas.consumir_mensajes('cola')

          while self.corriendo:
            vuelo, estado = self._protocolo.recibir_vuelo()
            if estado == Estado.OK:
                self.procesar_vuelo(vuelo)
            else:
                break

            if estado == Estado.FIN_VUELOS:
                self._protocoloVelocidad.enviar_fin_vuelos()

    
        
        
        
           
