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

from multiprocessing import Process
from protocolo_enviar_heartbeat import ProtocoloEnviarHeartbeat, IDENTIFICADOR_FILTRO_PRECIO
from socket_comun_udp import SocketComunUDP

REGISTROS_EN_MEMORIA = 1000

class FiltroPrecios:
    
    def __init__(self, id, cant_watchdogs, periodo_heartbeat, host_watchdog, port_watchdog):
       self._protocolo = ProtocoloFiltroPrecio()
       self._id = id
       self.inicialar()   
       self.corriendo = True

       signal.signal(signal.SIGTERM, self.sigterm_handler)

       socket = SocketComunUDP()
       self._protocolo_heartbeat = ProtocoloEnviarHeartbeat(socket, host_watchdog, port_watchdog, cant_watchdogs,
        IDENTIFICADOR_FILTRO_PRECIO, periodo_heartbeat, id)
       
    def inicialar(self):
       self.archivos_por_trayecto = {}
       self.vuelos_por_trayecto_memoria = {}
       self.total_vuelos_por_trayecto = {}
       self.total_por_trayecto = {}
       self.promedio = 0.0
       self.cantidad = 0    
       self.vuelos_procesados = 0
       self.resultados_enviados = 0
 
        
    def procesar_vuelo(self, id_cliente, vuelos):        
        self.vuelos_procesados += 1;
        if (self.vuelos_procesados % 10) == 1:
            logging.info(f'Procesando Vuelo: {self.vuelos_procesados}')  
        for vuelo in vuelos:
            logging.info(f'Inicio el proceso cliente {id_cliente} origen { vuelo.origen } precio { vuelo.precio }')
            trayecto = f'{id_cliente}-{vuelo.origen}-{vuelo.destino}'
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
                for precio in self.vuelos_por_trayecto_memoria[trayecto]:
                    archivo = self.archivos_por_trayecto[trayecto]
                    precio_binario = struct.pack('f', precio)
                    archivo.write(precio_binario)
                self.vuelos_por_trayecto_memoria[trayecto] = []
            
    def cerrar_archivos(self, id_cliente):
        logging.info("Cerrando archivos")
        for trayecto, archivo in self.archivos_por_trayecto.items():
            if trayecto.startswith(id_cliente):
                archivo.close()

    def borrar_archivos(self, id_cliente):
        logging.info("Borrando archivos")
        for trayecto, archivo in self.archivos_por_trayecto.items():
            if trayecto.startswith(id_cliente):
                os.remove(trayecto)
        
    def procesar_finvuelo(self, id_cliente):        
        logging.info(f'Calculo el promedio y lo envia')
        # Guardo en disco lo que quedo en memoria de cada trayecto
        for trayecto, archivo in self.archivos_por_trayecto.items():
            if trayecto.startswith(id_cliente):
                for precio in self.vuelos_por_trayecto_memoria[trayecto]:
                    precio_binario = struct.pack('f', precio)
                    archivo.write(precio_binario)
                self.vuelos_por_trayecto_memoria[trayecto] = []
        self.cerrar_archivos(id_cliente)        
        promedio = 0
        cantidad = 0
        for trayecto, archivo in self.archivos_por_trayecto.items():
            if trayecto.startswith(id_cliente):
                promedio_nuevo = self.total_por_trayecto[trayecto] / self.total_vuelos_por_trayecto[trayecto]
                suma_vuelos = cantidad + self.total_vuelos_por_trayecto[trayecto]
                parte_actual = cantidad / suma_vuelos
                parte_nueva = self.total_vuelos_por_trayecto[trayecto] / suma_vuelos
                npromedio = (promedio * parte_actual) + (promedio_nuevo * parte_nueva)        
                promedio = npromedio
                cantidad = suma_vuelos
        self._protocolo.enviar_promedio(id_cliente, promedio, cantidad)
        
    def procesar_promediogeneral(self, id_cliente, promedio):
        logging.info(f"Recibe el promedio {promedio} del cliente {id_cliente}")
        self._protocoloResultado = ProtocoloResultadosServidor()
        
        resultados = []
        for trayecto, vuelos in self.total_vuelos_por_trayecto.items():            
            if trayecto.startswith(id_cliente):
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
                            suma_precios_por_encima += precio
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
                    resultados.append(res)
                    
                    logging.debug(f"Filtro enviando resultado: {trayecto} promedio: {precio_promedio}")
            
        logging.info(f'Resultados enviados: {self.resultados_enviados}')
        self._protocoloResultado.enviar_resultado_filtro_precio(resultados, id_cliente)
        self._protocoloResultado.enviar_fin_resultados_filtro_precio(id_cliente)
        self.borrar_archivos(id_cliente)

    def run(self):
        try:
          self._handle_protocolo_heartbeat = Process(target=self._protocolo_heartbeat.enviar_heartbeats)  
          self._handle_protocolo_heartbeat.start()
          self._protocolo.iniciar(self.procesar_vuelo, self.procesar_finvuelo, self.procesar_promediogeneral, self._id)
        except Exception as e:
            logging.error(f'Ocurrió una excepción: {e}')
            self.cerrar()
          

    def sigterm_handler(self, _signo, _stack_frame):
        logging.error('SIGTERM recibida')
        self.cerrar()
        

    def cerrar(self):
        logging.error('Cerrando recursos')
        self._protocolo.cerrar()
        if hasattr(self, '_protocoloResultado'):
            self._protocoloResultado.cerrar()

        self.cerrar_archivos()

        if hasattr(self, '_protocolo_heartbeat'):
            self._protocolo_heartbeat.cerrar()

        if hasattr(self, '_handle_protocolo_heartbeat'):
            if self._handle_protocolo_heartbeat.is_alive():
                self._handle_protocolo_heartbeat.terminate()
            self._handle_protocolo_heartbeat.join()
