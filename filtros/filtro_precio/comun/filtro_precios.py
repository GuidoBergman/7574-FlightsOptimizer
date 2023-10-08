import logging
from math import log
import signal

from manejador_colas import ManejadorColas
from modelo.estado import Estado
from modelo.Vuelo import Vuelo
from modelo.ResultadoFiltroEscalas import ResultadoFiltroEscalas


from protocolofiltroprecio import ProtocoloFiltroPrecio
from protocolo_resultados_servidor import ProtocoloResultadosServidor


class FiltroPrecios:
    def __init__(self, id):
       signal.signal(signal.SIGTERM, self.sigterm_handler)
       self._protocolo = ProtocoloFiltroPrecio()
       self._protocoloResultado = ProtocoloResultadosServidor()
       self.precios_por_trayecto = {}
       self.promedio_por_trayecto = {}
       
       self.corriendo = True

       self._id = id
        
    def sigterm_handler(self, _signo, _stack_frame):
        self._protocolo.parar()
        logging.info('action: sigterm_received')

        
    def procesar_vuelo(self, vuelo: Vuelo):
        
        logging.info(f'Inicio el proceso origen { vuelo.origen } precio { vuelo.precio }')
        trayecto = f'{vuelo.origen}{vuelo.destino}'
        if trayecto not in self.precios_por_trayecto:
            # Si el trayecto no existe en el diccionario, creamos una lista con el precio actual
            self.precios_por_trayecto[trayecto] = [vuelo.precio]
            self.promedio_por_trayecto[trayecto] = vuelo.precio  # Inicializamos el promedio con el primer precio
        else:
            # Si el trayecto ya existe en el diccionario, simplemente agregamos el precio a la lista
            self.precios_por_trayecto[trayecto].append(vuelo.precio)
            # Calculamos el nuevo promedio basado en el valor actual, la cantidad de elementos y el nuevo valor
            n = len(self.precios_por_trayecto[trayecto])
            self.promedio_por_trayecto[trayecto] = (self.promedio_por_trayecto[trayecto] * ((n - 1) / n)) + (vuelo.precio / n)
            logging.info(f'trayecto {trayecto} promedio calculado {self.promedio_por_trayecto[trayecto]}')
            
        logging.info(f'Procesado el trayecto { trayecto } precio { vuelo.precio }')
        
 
    def procesar_finvuelo(self):        
        logging.info(f'FIN DE VUELOS')

    def run(self):
          self._protocolo.iniciar(self.procesar_vuelo, self.procesar_finvuelo)
          