import logging
from math import log
import signal
import sys
import traceback

from manejador_colas import ManejadorColas
from modelo.estado import Estado
from modelo.Vuelo import Vuelo
from protocolofiltroprecio import ProtocoloFiltroPrecio
from comun.promedio_cliente import PromedioCliente

from multiprocessing import Process
from protocolo_enviar_heartbeat import ProtocoloEnviarHeartbeat, IDENTIFICADOR_CALCULADOR_PROMEDIO
from socket_comun_udp import SocketComunUDP


class CalculadorPromedio:
    def __init__(self, cant_filtros_precio, cant_watchdogs, periodo_heartbeat, host_watchdog, port_watchdog):
       self._protocolo = ProtocoloFiltroPrecio()
       signal.signal(signal.SIGTERM, self.sigterm_handler)
       self.cant_filtros_precio = cant_filtros_precio
       self.clientes = {}
       socket = SocketComunUDP()
       self._protocolo_heartbeat = ProtocoloEnviarHeartbeat(socket, host_watchdog, port_watchdog, cant_watchdogs,
        IDENTIFICADOR_CALCULADOR_PROMEDIO, periodo_heartbeat)
        
        
    def procesar_promedio(self, id_cliente, promedio: float, cantidad: int):
        if cantidad > 0:
            if id_cliente in self.clientes:
                self.clientes[id_cliente].agregar(promedio, cantidad)
            else:
                self.clientes[id_cliente] = PromedioCliente(promedio, cantidad)
        
        logging.info(f"Procesa promedio {id_cliente} : | promedio { promedio} cantidad { cantidad}")
        promCliente = self.clientes[id_cliente]
        logging.info(f"Recibidos {promCliente.recibidos} / { self.cant_filtros_precio} ")
        if (promCliente.recibidos >= self.cant_filtros_precio):
            logging.info(f"Envia promedio: {promCliente.promedio}")
            self._protocolo.enviar_promediogeneral(id_cliente, promCliente.promedio)
            del self.clientes[id_cliente]

        informacion_a_persistir = str(promedio) + ',' + str(cantidad)
        return informacion_a_persistir

    def run(self):
          logging.info(f"Iniciando promedios")  
          try:
            self._handle_protocolo_heartbeat = Process(target=self._protocolo_heartbeat.enviar_heartbeats)  
            self._handle_protocolo_heartbeat.start()
            for id_cliente, linea in self._protocolo.recuperar_siguiente_linea():
                logging.info(f'Recuperé la linea {linea} del cliente {id_cliente}')
            self._protocolo.iniciar_promedio(self.procesar_promedio)
          except Exception as e:
            logging.error(f'Ocurrió una excepción: {e}')
            exc = sys.exception()
            traceback.print_tb(exc.__traceback__, limit=1, file=sys.stdout)          
            traceback.print_exception(exc, limit=2, file=sys.stdout)
            self.cerrar()
          
    
    def sigterm_handler(self, _signo, _stack_frame):
        logging.error('SIGTERM recibida')
        self.cerrar()
        

    def cerrar(self):
        logging.error('Cerrando recursos')
        self._protocolo.cerrar()

        if hasattr(self, '_protocolo_heartbeat'):
            self._protocolo_heartbeat.cerrar()

        if hasattr(self, '_handle_protocolo_heartbeat'):
            if self._handle_protocolo_heartbeat.is_alive():
                self._handle_protocolo_heartbeat.terminate()
            self._handle_protocolo_heartbeat.join()
        
