import logging
from math import log
import signal

from manejador_colas import ManejadorColas
from modelo.estado import Estado
from modelo.Vuelo import Vuelo
from protocolofiltroprecio import ProtocoloFiltroPrecio

from multiprocessing import Process
from protocolo_enviar_heartbeat import ProtocoloEnviarHeartbeat, IDENTIFICADOR_CALCULADOR_PROMEDIO
from socket_comun_udp import SocketComunUDP


class CalculadorPromedio:
    def __init__(self, cant_filtros_precio, cant_watchdogs, periodo_heartbeat, host_watchdog, port_watchdog):
       self._protocolo = ProtocoloFiltroPrecio()
       signal.signal(signal.SIGTERM, self.sigterm_handler)
       self.corriendo = True
       self.cant_filtros_precio = cant_filtros_precio
       self.promedio = 0.0
       self.cantidad = 0
       self.recibidos = 0

       socket = SocketComunUDP()
       self._protocolo_heartbeat = ProtocoloEnviarHeartbeat(socket, host_watchdog, port_watchdog, cant_watchdogs,
        IDENTIFICADOR_CALCULADOR_PROMEDIO, periodo_heartbeat)
        
        
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
          try:
            self._handle_protocolo_heartbeat = Process(target=self._protocolo_heartbeat.enviar_heartbeats)  
            self._handle_protocolo_heartbeat.start()
            self._protocolo.iniciar_promedio(self.procesar_promedio)
          except:
            if self._handle_protocolo_heartbeat:
                self._handle_protocolo_heartbeat.terminate()
                self._handle_protocolo_heartbeat.join()
          
    
    def sigterm_handler(self, _signo, _stack_frame):
        logging.error('SIGTERM recibida')
        self._protocolo.cerrar()

        if self._protocolo_heartbeat:
            self._protocolo_heartbeat.cerrar()

        if self._handle_protocolo_heartbeat:
            self._handle_protocolo_heartbeat.terminate()
            self._handle_protocolo_heartbeat.join()
        
