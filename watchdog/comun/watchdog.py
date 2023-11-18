import logging
import signal
from protocolo_recibir_heartbeat import (ProtocoloRecibirHeartbeat,
    IDENTIFICADOR_SERVER, IDENTIFICADOR_FILTRO_VELOCIDAD, IDENTIFICADOR_FILTRO_ESCALAS, 
    IDENTIFICADOR_FILTRO_DISTANCIA, IDENTIFICADOR_FILTRO_PRECIO, IDENTIFICADOR_CALCULADOR_PROMEDIO,
    IDENTIFIFICADOR_WATCHDOG, STATUS_OK, STATUS_TIMEOUT)
from socket_comun_udp import SocketComunUDP
from time import time

from multiprocessing import Process

from protocolo_enviar_heartbeat import ProtocoloEnviarHeartbeat


NOMBRE_COMPLETO_SERVER = 'server'
NOMBRE_COMPLETO_FILTRO_VELOCIDAD = 'filtro_velocidad'
NOMBRE_COMPLETO_FILTRO_ESCALAS = 'filtro_escalas'
NOMBRE_COMPLETO_FILTRO_DISTANCIA = 'filtro_distancia'
NOMBRE_COMPLETO_FILTRO_PRECIO = 'filtro_precio'
NOMBRE_COMPLETO_CALCULADOR_PROMEDIO = 'calculador_promedio'
NOMBRE_COMPLETO_WATCHDOG = 'watchdog'

class Watchdog:
    def __init__(self, timeout_un_mensaje, max_timeout, id, 
    cant_filtros_escalas, cant_filtros_distancia, cant_filtros_velocidad, cant_filtros_precio,
    cant_watchdogs, periodo_heartbeat, host_watchdog, port_watchdog):
        signal.signal(signal.SIGTERM, self.sigterm_handler)
        socket = SocketComunUDP()
        socket.bind('', port_watchdog)
        socket.set_timeout(timeout_un_mensaje)
        self._protocolo_recibir = ProtocoloRecibirHeartbeat(socket)    

        self._max_timeout = max_timeout
        self._id = id
        self._cant_watchdogs = cant_watchdogs

        self._crear_timestamps_ultimos_heartbeats(cant_filtros_escalas, cant_filtros_distancia, cant_filtros_velocidad, 
            cant_filtros_precio, cant_watchdogs)
        
        self._protocolo_enviar = ProtocoloEnviarHeartbeat(socket, host_watchdog, port_watchdog, cant_watchdogs,
        IDENTIFIFICADOR_WATCHDOG, periodo_heartbeat, id)


    def _crear_timestamps_ultimos_heartbeats(self, cant_filtros_escalas, cant_filtros_distancia, cant_filtros_velocidad, 
        cant_filtros_precio, cant_watchdogs):
        self._timestamps_ultimos_heartbeats = {}
        timestamp = time()
        self._timestamps_ultimos_heartbeats[IDENTIFICADOR_SERVER] = timestamp
        self._timestamps_ultimos_heartbeats[IDENTIFICADOR_CALCULADOR_PROMEDIO] = timestamp
        for i in range(1, cant_filtros_escalas+1):
            self._timestamps_ultimos_heartbeats[IDENTIFICADOR_FILTRO_ESCALAS + str(i)] = timestamp
        for i in range(1, cant_filtros_distancia+1):
            self._timestamps_ultimos_heartbeats[IDENTIFICADOR_FILTRO_DISTANCIA + str(i)] = timestamp
        for i in range(1, cant_filtros_velocidad+1):
            self._timestamps_ultimos_heartbeats[IDENTIFICADOR_FILTRO_VELOCIDAD + str(i)] = timestamp
        for i in range(1, cant_filtros_precio+1):
            self._timestamps_ultimos_heartbeats[IDENTIFICADOR_FILTRO_PRECIO + str(i)] = timestamp
        for i in range(1, cant_watchdogs+1):
            if i != self._id:
                self._timestamps_ultimos_heartbeats[IDENTIFIFICADOR_WATCHDOG + str(i)] = timestamp  
        
        logging.debug(f'Diccionario timestamps {self._timestamps_ultimos_heartbeats}')
        
    def run(self):
          logging.info(f"Iniciando watchdog")  
          try:
            self._handle_protocolo_enviar = Process(target=self._protocolo_enviar.enviar_heartbeats, args=(self._id,))  
            self._handle_protocolo_enviar.start()
            self._run()
          except Exception as e:
            logging.error(f'Ocurrió una excepción: {e}')
            self.cerrar()





    def _run(self):
          while True:
            estado, identificador_tipo, numero = self._protocolo_recibir.recibir_hearbeat()
            if estado == STATUS_OK:
                logging.debug(f"Recibí un heartbeat  | identificador tipo: {identificador_tipo} | numero: {numero}")
                timestamp = time()

                if numero:
                    self._timestamps_ultimos_heartbeats[identificador_tipo + str(numero)] = timestamp
                else:
                    self._timestamps_ultimos_heartbeats[identificador_tipo] = timestamp

            elif estado == STATUS_TIMEOUT:
                logging.info(f"Timeout en la recepción de heartbeats")

            self._revivir_muertos()
            
            



    def _revivir_muertos(self):
        for identificador, ultimo_timestamp in self._timestamps_ultimos_heartbeats.items():
            timestamp = time()
            # Si está muerto (porque pasó el timeout y no recibí heartbeats)
            if timestamp - ultimo_timestamp > self._max_timeout:
                nombre_completo = self._identificador_a_nombre_completo(identificador)
                if self._debo_revivirlo(nombre_completo):
                    logging.info(f"Murió {nombre_completo} y lo tengo que revivir yo")
                else:
                    logging.info(f"Murió {nombre_completo}, pero no lo tengo que revivir yo")

    
    def _identificador_a_nombre_completo(self, identificador):
        if identificador[0] == IDENTIFICADOR_SERVER:
            return NOMBRE_COMPLETO_SERVER
        elif identificador[0] == IDENTIFICADOR_FILTRO_VELOCIDAD:
            return NOMBRE_COMPLETO_FILTRO_VELOCIDAD + str(identificador [1])
        elif identificador[0] == IDENTIFICADOR_FILTRO_ESCALAS:
            return NOMBRE_COMPLETO_FILTRO_ESCALAS + str(identificador [1])
        elif identificador[0] == IDENTIFICADOR_FILTRO_DISTANCIA:
            return NOMBRE_COMPLETO_FILTRO_DISTANCIA + str(identificador [1])
        elif identificador[0] == IDENTIFICADOR_FILTRO_PRECIO:
            return NOMBRE_COMPLETO_FILTRO_PRECIO + str(identificador [1])
        elif identificador[0] == IDENTIFICADOR_CALCULADOR_PROMEDIO:
            return NOMBRE_COMPLETO_CALCULADOR_PROMEDIO
        elif identificador[0] == IDENTIFIFICADOR_WATCHDOG:
            return NOMBRE_COMPLETO_WATCHDOG + str(identificador [1])
        else:
            return None

    def _debo_revivirlo(self, nombre):
        # Si es un watchdog, lo revivo solo si numero es el siguiente al mio
        if nombre.startswith(NOMBRE_COMPLETO_WATCHDOG):
            numero_otro = nombre[-1:]
            return self._id +1 == int(numero_otro) or self._id+1-self._cant_watchdogs == int(numero_otro)
        else:
            watchdog_que_lo_revive = (hash(nombre) % self._cant_watchdogs) + 1
            if watchdog_que_lo_revive == self._id:
                return True
            else: 
                return False

          
    
    def sigterm_handler(self, _signo, _stack_frame):
        logging.error('SIGTERM recibida')
        self.cerrar()
        

    def cerrar(self):
        logging.error('Ejecutando cerrar')
        self._protocolo_enviar.cerrar()
        self._protocolo_recibir.cerrar()

        if self._handle_protocolo_enviar and self._handle_protocolo_enviar.is_alive():
            self._handle_protocolo_enviar.terminate()
            self._handle_protocolo_enviar.join()
        
