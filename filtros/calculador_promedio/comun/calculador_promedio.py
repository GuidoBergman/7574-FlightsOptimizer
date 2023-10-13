import logging
from math import log
import signal

from manejador_colas import ManejadorColas
from modelo.estado import Estado
from modelo.Vuelo import Vuelo
from protocolofiltroprecio import ProtocoloFiltroPrecio


class CalculadorPromedio:
    def __init__(self, cant_filtros_precio):
       self._protocolo = ProtocoloFiltroPrecio()
       signal.signal(signal.SIGTERM, self.sigterm_handler)
       self.corriendo = True
       self.cant_filtros_precio = cant_filtros_precio
       self.promedio = 0.0
       self.cantidad = 0
       self.recibidos = 0
        
        
    def procesar_promedio(self, promedio: float, cantidad: int):
        if cantidad > 0:
            parte_actual = self.cantidad / (self.cantidad + cantidad)
            parte_nueva = cantidad / (self.cantidad + cantidad)
            npromedio = (self.promedio * parte_actual) + (promedio * parte_nueva)        
            self.promedio = npromedio
            self.cantidad += cantidad
        
        self.recibidos += 1
        
        logging.info(f"Procesa promedio: {self.recibidos} / {self.cant_filtros_precio} | promedio {self.promedio} cantidad {self.cantidad}")
        if (self.recibidos >= self.cant_filtros_precio):
            logging.info(f"Envia promedio: {self.promedio}")
            self._protocolo.enviar_promediogeneral(self.promedio)
        
        
    def run(self):
          logging.info(f"Iniciando promedios")  
          self._protocolo.iniciar_promedio(self.procesar_promedio)
          
    
    def sigterm_handler(self, _signo, _stack_frame):
        logging.error('SIGTERM recibida')
        self._protocolo.cerrar()
        
