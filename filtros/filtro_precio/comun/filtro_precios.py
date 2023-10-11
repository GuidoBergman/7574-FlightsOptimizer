import logging
import os
from math import log
import signal
import struct

from manejador_colas import ManejadorColas
from modelo.estado import Estado
from modelo.Vuelo import Vuelo
from modelo.ResultadoEstadisticasPrecios import ResultadoEstadisticaPrecios


from protocolofiltroprecio import ProtocoloFiltroPrecio
from protocolo_resultados_servidor import ProtocoloResultadosServidor


class FiltroPrecios:
    def __init__(self, id):
       signal.signal(signal.SIGTERM, self.sigterm_handler)
       self._protocolo = ProtocoloFiltroPrecio()
       self._protocoloResultado = ProtocoloResultadosServidor()
       self._id = id
       self.inicialar()   
       self.corriendo = True
       
    def inicialar(self):
       self.archivos_por_trayecto = {}
       self.vuelos_por_trayecto = {}
       self.total_por_trayecto = {}
       self.promedio = 0.0
       self.cantidad = 0    

        
    def sigterm_handler(self, _signo, _stack_frame):
        self.cerrar_archivos()
        self.borrar_archivos()
        self._protocolo.parar()
        logging.info('action: sigterm_received')

        

        
    def agregar_promedio(self, promedio: float, cantidad: int):
        parte_actual = self.cantidad / (self.cantidad + cantidad)
        parte_nueva = cantidad / (self.cantidad + cantidad)
        npromedio = (self.promedio * parte_actual) + (promedio * parte_nueva)        
        self.promedio = npromedio
        self.cantidad += cantidad
        
        
    def procesar_vuelo(self, vuelo: Vuelo):
        
        logging.debug(f'Inicio el proceso origen { vuelo.origen } precio { vuelo.precio }')
        trayecto = f'{vuelo.origen}-{vuelo.destino}'
        if trayecto not in self.archivos_por_trayecto:
            # Si el trayecto no existe en el diccionario, creamos una lista con el precio actual
            archivo = open(trayecto, 'ab')
            self.archivos_por_trayecto[trayecto] = open(trayecto, 'ab')
            self.total_por_trayecto[trayecto] = vuelo.precio
            self.vuelos_por_trayecto[trayecto] = 1
        else:
            archivo = self.archivos_por_trayecto[trayecto]
            self.total_por_trayecto[trayecto] += vuelo.precio  
            self.vuelos_por_trayecto[trayecto] += 1              

        # Escribe el precio en el archivo en formato binario
        precio_binario = struct.pack('f', vuelo.precio)
        archivo.write(precio_binario)
        
 
    def cerrar_archivos(self):
        for trayecto, archivo in self.archivos_por_trayecto.items():
            archivo.close()

    def borrar_archivos(self):
        for trayecto, archivo in self.archivos_por_trayecto.items():
            os.remove(trayecto)
            
    def procesar_finvuelo(self):        
        logging.info(f'Calculo el promedio y total')
        self.cerrar_archivos()
        for trayecto, archivo in self.archivos_por_trayecto.items():
            self.agregar_promedio(self.total_por_trayecto[trayecto] / self.vuelos_por_trayecto[trayecto], self.vuelos_por_trayecto[trayecto])
        self._protocolo.enviar_promedio(self.promedio, self.cantidad)
        
        
    def procesar_promediogeneral(self, promedio):
        logging.info(f"Envia resultados para el promedio {promedio}")
        
        for trayecto, vuelos in self.vuelos_por_trayecto.items():
            
            logging.info(f"Abro archivos para trayecto {trayecto}")                 
            with open(trayecto, 'rb') as archivo:
                precios_por_encima = []
                precio_binario = archivo.read(4)  # Leer 4 bytes a la vez (tama�o de un float)
                while precio_binario:
                    precio = struct.unpack('f', precio_binario)[0]
                    if precio > promedio:
                        precios_por_encima.append(precio)
                    precio_binario = archivo.read(4)

            # Calcula el precio promedio solo para los precios por encima de 'promedio'
            if precios_por_encima:
                precio_promedio = sum(precios_por_encima) / len(precios_por_encima)
                precio_maximo = max(precios_por_encima)
                res = ResultadoEstadisticaPrecios(trayecto, precio_promedio, precio_maximo)
                logging.info(f"Filtro enviando resultado: {trayecto} promedio: {precio_promedio}")
                self._protocoloResultado.enviar_resultado_filtro_precio(res)
            
        self._protocoloResultado.enviar_fin_resultados_filtro_precio()
        self.borrar_archivos()

        


    def run(self):
          self._protocolo.iniciar(self.procesar_vuelo, self.procesar_finvuelo, self.procesar_promediogeneral, self._id)
          