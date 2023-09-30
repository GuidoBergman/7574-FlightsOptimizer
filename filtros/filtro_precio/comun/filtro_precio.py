import logging
import signal
from filtros.filtro_precio.comun import resumen_precios
from filtros.filtro_precio.comun.resumen_precios import ResumenPrecios
from filtros.modelo.Vuelo import Vuelo
from manejador_colas import ManejadorColas


class FiltroPrecios:
    def __init__(self, port, listen_backlog):
       self._colas = ManejadorColas('rabbitmq')
       signal.signal(signal.SIGTERM, self.sigterm_handler)
        
        
    def mandar_resultado(self, trayecto, resumen: ResumenPrecios):
        # Lógica para manejar el resultado, por ejemplo, imprimirlo o enviarlo a otra parte.
        print(f"Resumen para trayecto {trayecto}:")
        print(f"Precio máximo: {resumen.maximo_precio}")
        print(f"Suma de precios: {resumen.suma_precios}")
        print(f"Total de vuelos: {resumen.total_vuelos}")

        

    def sigterm_handler(self, _signo, _stack_frame):
        logging.info('action: sigterm_received')

        
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
            self.mandar_resultado(trayecto, resumen)
        
    def run(self):
          self._colas.crear_cola('cola')
          self._colas.consumir_mensajes('cola')



            
    
        
        
        
           
