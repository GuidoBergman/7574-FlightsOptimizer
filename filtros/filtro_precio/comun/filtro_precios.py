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


REGISTROS_EN_MEMORIA = 10000

class FiltroPrecios:
    
    def __init__(self, id):
       self._protocolo = ProtocoloFiltroPrecio()
       self._id = id
       self.inicialar()   
       self.corriendo = True
       
    def inicialar(self):
       self.archivos_por_trayecto = {}
       self.vuelos_por_trayecto_memoria = {}
       self.total_vuelos_por_trayecto = {}
       self.total_por_trayecto = {}
       self.promedio = 0.0
       self.cantidad = 0    
       self.vuelos_procesados = 0
       self.resultados_enviados = 0

        
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
        
        self.vuelos_procesados += 1;
        if (self.vuelos_procesados % 3000) == 1:
            logging.info(f'Procesando Vuelo: {self.vuelos_procesados}')  
        
        logging.debug(f'Inicio el proceso origen { vuelo.origen } precio { vuelo.precio }')
        trayecto = f'{vuelo.origen}-{vuelo.destino}'
        if trayecto not in self.archivos_por_trayecto:
            # Si el trayecto no existe en el diccionario, creamos una lista con el precio actual
            archivo = open(trayecto, 'ab')
            self.vuelos_por_trayecto_memoria[trayecto] = []
            self.archivos_por_trayecto[trayecto] = archivo            
            
            self.vuelos_por_trayecto_memoria[trayecto] = []            
            self.total_por_trayecto[trayecto] = 0
            self.total_vuelos_por_trayecto[trayecto] = 0
        else:
            archivo = self.archivos_por_trayecto[trayecto]
        
        
        # Procesa el vuelo
        self.vuelos_por_trayecto_memoria[trayecto].append(vuelo.precio)
        self.total_por_trayecto[trayecto] += vuelo.precio
        self.total_vuelos_por_trayecto[trayecto] += 1
        
        #Si hay muchos en memoria lo guarda a un archivo
        if len(self.vuelos_por_trayecto_memoria[trayecto]) > REGISTROS_EN_MEMORIA:
            for precios in self.vuelos_por_trayecto_memoria[trayecto]:
                archivo = self.archivos_por_trayecto[trayecto]
                for precio in precios: 
                    precio_binario = struct.pack('f', precio)
                    archivo.write(precio_binario)
            self.vuelos_por_trayecto_memoria[trayecto] = []
            
        
        
 
    def cerrar_archivos(self):
        logging.info("Cerrando archivos")
        for trayecto, archivo in self.archivos_por_trayecto.items():
            archivo.close()

    def borrar_archivos(self):        
        logging.info("Borrando archivos")
        for trayecto, archivo in self.archivos_por_trayecto.items():
            os.remove(trayecto)
            
    def procesar_finvuelo(self):        
        logging.info(f'Calculo el promedio y lo envia')
        # Guardo en disco lo que quedo en memoria de cada trayecto
        for trayecto, archivo in self.archivos_por_trayecto.items():
            for precio in self.vuelos_por_trayecto_memoria[trayecto]:
                precio_binario = struct.pack('f', precio)
                archivo.write(precio_binario)
            self.vuelos_por_trayecto_memoria[trayecto] = []
        self.cerrar_archivos()
        

        for trayecto, archivo in self.archivos_por_trayecto.items():
            self.agregar_promedio(self.total_por_trayecto[trayecto] / self.total_vuelos_por_trayecto[trayecto], self.total_vuelos_por_trayecto[trayecto])
        self._protocolo.enviar_promedio(self.promedio, self.cantidad)
        
        
        
    def procesar_promediogeneral(self, promedio):
        logging.info(f"Recibe el promedio {promedio}")
        
        self._protocoloResultado = ProtocoloResultadosServidor()
        for trayecto, vuelos in self.total_vuelos_por_trayecto.items():
            
            precios_por_encima = 0
            suma_precios_por_encima = 0 
            precio_maximo = 0
            logging.debug(f"Abro archivos para trayecto {trayecto}")            
            with open(trayecto, 'rb') as archivo:
                
                precio_binario = archivo.read(4)  # Leer 4 bytes a la vez (tamaño de un float)
                while precio_binario:
                    precio = struct.unpack('f', precio_binario)[0]
                    if precio > promedio:
                        precios_por_encima += 1
                        suma_precios_por_encima = precio
                        if precio_maximo < precio:
                            precio_maximo = precio
                    
                    precio_binario = archivo.read(4)

            # Si hay precios por encima de 'promedio' los envia
            if precios_por_encima > 0:
                precio_promedio = suma_precios_por_encima / precios_por_encima 
                res = ResultadoEstadisticaPrecios(trayecto, precio_promedio, precio_maximo)

                self.resultados_enviados += 1
                if (self.resultados_enviados % 100) == 1:
                    logging.info(f'Enviando resultados: {self.resultados_enviados}')
                    
                logging.debug(f"Filtro enviando resultado: {trayecto} promedio: {precio_promedio}")
                self._protocoloResultado.enviar_resultado_filtro_precio(res)
        logging.info(f'Resultados enviados: {self.resultados_enviados}')
        self._protocoloResultado.enviar_fin_resultados_filtro_precio()
        self.borrar_archivos()

        


    def run(self):
          self._protocolo.iniciar(self.procesar_vuelo, self.procesar_finvuelo, self.procesar_promediogeneral, self._id)
          

    def sigterm_handler(self, _signo, _stack_frame):
        logging.info('SIGTER recibida')
        self._protocolo.cerrar()
        if self._protocoloResultado:
            self._protocoloResultado.cerrar()

        self.cerrar_archivos()