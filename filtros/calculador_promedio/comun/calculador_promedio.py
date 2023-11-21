import logging
from math import log
import signal

from manejador_colas import ManejadorColas
from modelo.estado import Estado
from modelo.Vuelo import Vuelo
from protocolofiltroprecio import ProtocoloFiltroPrecio
from comun.promedio_cliente import PromedioCliente


class CalculadorPromedio:
    def __init__(self, cant_filtros_precio):
       signal.signal(signal.SIGTERM, self.sigterm_handler)
       self._protocolo = ProtocoloFiltroPrecio()
       self.cant_filtros_precio = cant_filtros_precio
       self.corriendo = True
       self.promedios = {}
        
    def procesar_promedio(self, id_cliente, promedio: float, cantidad: int):
        logging.info(f"Procesa promedio Cliente  {id_cliente} | Promedio: {promedio} | Cantidad: {cantidad}")
        if (id_cliente in self.promedios):
            self.promedios[id_cliente].agregar(promedio, cantidad)
        else:
            self.promedios[id_cliente] = PromedioCliente(id_cliente, promedio, cantidad)
            
        promedio_cliente = self.promedios[id_cliente]
        if (promedio_cliente.recibidos >= self.cant_filtros_precio):
            logging.info(f"Envia promedio: {promedio_cliente.promedio}")
            self._protocolo.enviar_promediogeneral(id_cliente, promedio_cliente.promedio)
            del self.promedios[id_cliente]
        
    def run(self):
          logging.info(f"Iniciando promedios")  
          self._protocolo.iniciar_promedio(self.procesar_promedio)
          
    
    def sigterm_handler(self, _signo, _stack_frame):
        logging.error('SIGTERM recibida')
        self._protocolo.cerrar()
        
