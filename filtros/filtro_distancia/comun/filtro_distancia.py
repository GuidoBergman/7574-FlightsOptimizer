import logging
import signal
from filtros.modelo.Vuelo import Vuelo
from manejador_colas import ManejadorColas


class FiltroDistancia:
    def __init__(self, port, listen_backlog):
       self._colas = ManejadorColas('rabbitmq')
       signal.signal(signal.SIGTERM, self.sigterm_handler)
        
        

        

    def sigterm_handler(self, _signo, _stack_frame):
        logging.info('action: sigterm_received')

        
    def procesar_vuelo(vuelo: Vuelo):
        logging.info('procesando vuelo')



        
    def run(self):
          self._colas.crear_cola('cola')
          self._colas.consumir_mensajes('cola')



            
    
        
        
        
           
