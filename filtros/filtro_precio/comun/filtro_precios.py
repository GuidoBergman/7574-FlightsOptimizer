import logging
from math import log
import signal

from manejador_colas import ManejadorColas
from modelo.estado import Estado
from modelo.Vuelo import Vuelo
from modelo.ResultadoEstadisticasPrecios import ResultadoEstadisticaPrecios


from protocolofiltroprecio import ProtocoloFiltroPrecio
from protocolo_resultados_servidor import ProtocoloResultadosServidor


class FiltroPrecios:
    def __init__(self):
       signal.signal(signal.SIGTERM, self.sigterm_handler)
       self._protocolo = ProtocoloFiltroPrecio()
       self._protocoloResultado = ProtocoloResultadosServidor()

       self.precios_por_trayecto = {}
       self.promedio_por_trayecto = {}
       self.promedio = 0.0
       self.cantidad = 0
       
       self.corriendo = True
        
    def sigterm_handler(self, _signo, _stack_frame):
        self._protocolo.parar()
        logging.info('action: sigterm_received')

        

        
    def agregar_promedio(self, promedio: float, cantidad: int):
        parte_actual = self.cantidad / (self.cantidad + cantidad)
        parte_nueva = cantidad / (self.cantidad + cantidad)
        npromedio = (self.promedio * parte_actual) + (promedio * parte_nueva)
        
        self.promedio = npromedio
        self.cantidad += cantidad
        
        
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
        logging.info(f'Calculo el promedio y total')
        for trayecto, precios in self.precios_por_trayecto.items():
            self.agregar_promedio(self.promedio_por_trayecto[trayecto], len(precios))
            
        self._protocolo.enviar_promedio(self.promedio, self.cantidad)
        
    def procesar_promediogeneral(self, promedio):
        logging.info(f"Envia resultados para el promedio {promedio}")
        
        for trayecto, precios in self.precios_por_trayecto.items():
                        
            precios_por_encima = [precio for precio in precios if precio > promedio]
            
            # Calcula el precio promedio solo para los precios por encima de 'promedio'
            if precios_por_encima:
                precio_promedio = sum(precios_por_encima) / len(precios_por_encima)
                precio_maximo = max(precios_por_encima)
                res = ResultadoEstadisticaPrecios(trayecto, precio_promedio, precio_maximo)
                logging.info(f"Filtro enviando resultado: {trayecto} promedio: {precio_promedio}")
                self._protocoloResultado.enviar_resultado_filtro_precio(res)
            

        


    def run(self):
          self._protocolo.iniciar(self.procesar_vuelo, self.procesar_finvuelo, self.procesar_promediogeneral)
          