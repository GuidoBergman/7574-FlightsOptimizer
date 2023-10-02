import logging
import signal
from filtros.filtro_precio.comun import resumen_precios
from filtros.filtro_precio.comun.resumen_precios import ResumenPrecios
from manejador_colas import ManejadorColas
from modelo.Vuelo import Vuelo
from modelo.estado_vuelo import EstadoVuelo
from protocolofiltroprecio import ProtocoloFiltroPrecio


class FiltroPrecios:
    def __init__(self, port, listen_backlog):
       self._colas = ManejadorColas('rabbitmq')
       self._protocolo = ProtocoloFiltroPrecio()
       self.corriendo = True
       signal.signal(signal.SIGTERM, self.sigterm_handler)
        
        

        

    def sigterm_handler(self, _signo, _stack_frame):
        self.corriendo = False

        
    def procesar_vuelo(self, vuelo: Vuelo):
       trayecto = vuelo.trayecto
       precio = vuelo.precio

       if trayecto in self.resumen_por_trayecto:
           resumen_actual = self.resumen_por_trayecto[trayecto]
           resumen_actual.actualizar(precio)
       else:
           resumen_actual = resumen_precios()
           resumen_actual.actualizar(precio)
           self.resumen_por_trayecto[trayecto] = resumen_actual


    
        
    def procesar_finvuelo(self):
        for trayecto, resumen in self.resumen_por_trayecto.items():
            self._protocolo.mandar_resumen(trayecto, resumen)
        
    def run(self):
          self._colas.crear_cola('cola')
          self._colas.consumir_mensajes('cola')
          
          while self.corriendo:
            vuelo, estado = self._protocolo.recibir_vuelo()
            if estado == EstadoVuelo.OK:
                self.procesar_vuelo(vuelo)
            elif estado == EstadoVuelo.FIN_VUELOS:
                self.procesar_finvuelo()
                break
            else:
                break
          




            
    
        
        
        
           
