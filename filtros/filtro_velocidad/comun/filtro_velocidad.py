import logging
import signal
import re
from protocolovelocidad import ProtocoloFiltroVelocidad
from modelo.ResultadoVuelosRapidos import ResultadoVuelosRapidos
from protocolo_resultados_servidor import ProtocoloResultadosServidor
import sys
import traceback

from multiprocessing import Process
from protocolo_enviar_heartbeat import ProtocoloEnviarHeartbeat, IDENTIFICADOR_FILTRO_VELOCIDAD
from socket_comun_udp import SocketComunUDP
from comun.recuperador_vuelos_rapidos import RecuperadorVuelosRapidos


class FiltroVelocidad:
    def __init__(self, id, cant_filtros_escalas, cant_watchdogs, periodo_heartbeat, host_watchdog, port_watchdog):
       self._protocolo = ProtocoloFiltroVelocidad()       
       self._protocoloResultado = ProtocoloResultadosServidor()    
       signal.signal(signal.SIGTERM, self.sigterm_handler)
       self.vuelos_mas_rapido_cliente = {}
       self._fines_vuelo = 0
        
       self._id = id
       self._cant_filtros_escalas = cant_filtros_escalas     
       self.resultados_enviados = 0
       self._recuperador_vuelos = RecuperadorVuelosRapidos()

       socket = SocketComunUDP()
       self._protocolo_heartbeat = ProtocoloEnviarHeartbeat(socket, host_watchdog, port_watchdog, cant_watchdogs,
        IDENTIFICADOR_FILTRO_VELOCIDAD, periodo_heartbeat, id)
 

        
    def calcular_minutos(self, duracion_str):
        minutos_totales = 0
        
        horas = 0
        minutos = 0

        match = re.match(r'PT(\d+)H?(\d*)M?$', duracion_str)            
        if match:
            horas = int(match.group(1))
            minutos = int(match.group(2)) if match.group(2) else 0
            minutos_totales += horas * 60 + minutos
        return minutos_totales

    def procesar_vuelo(self, id_cliente, vuelos):
        if id_cliente in self.vuelos_mas_rapido_cliente:
            vuelos_mas_rapido = self.vuelos_mas_rapido_cliente[id_cliente]
        else:
            logging.info(f"Agregando registros para cliente {id_cliente}")
            vuelos_mas_rapido = {}
                
        trayectos_con_cambios = []

        for vuelo in vuelos:
            trayecto = vuelo.origen + "-" + vuelo.destino
            # Obtener la duración del vuelo actual
            vuelo.duracion_enminutos = self.calcular_minutos(vuelo.duracion)
            logging.debug(f"Procesando vuelo trayecto: { trayecto } de duracion: {vuelo.duracion} en minutos: { vuelo.duracion_enminutos } ")
            # Comprobar si ya hay vuelos registrados para este trayecto
            vuelos_trayecto_old = None
            if trayecto in vuelos_mas_rapido:
                # Agregar el vuelo a la lista
                vuelos_trayecto_old = vuelos_mas_rapido[trayecto].copy()
                vuelos_mas_rapido[trayecto].append(vuelo)
            else:
                # Si no hay vuelos registrados para este proyecto, crear una lista con el vuelo actual
                vuelos_mas_rapido[trayecto] = [vuelo]

            # Si hay más de 2 vuelos para este proyecto, mantener solo los 2 más rápidos
            if len(vuelos_mas_rapido[trayecto]) > 2:
                vuelos_mas_rapido[trayecto].sort(key=lambda x: x.duracion_enminutos)
                vuelos_mas_rapido[trayecto] = vuelos_mas_rapido[trayecto][:2]

            vuelos_trayecto_new = vuelos_mas_rapido[trayecto]
            if self._hubo_cambios(vuelos_trayecto_old, vuelos_trayecto_new):
                logging.debug(f'Hubo cambios en un trayecto el trayecto {trayecto}, que pasó de {vuelos_trayecto_old} a {vuelos_trayecto_new}')
                trayectos_con_cambios.append(trayecto)

        self.vuelos_mas_rapido_cliente[id_cliente] = vuelos_mas_rapido

        contenido_a_persistir = self._recuperador_vuelos.obtener_contenido_a_persistir(vuelos_mas_rapido, trayectos_con_cambios)

        return contenido_a_persistir

    def _hubo_cambios(self, vuelos_trayecto_old, vuelos_trayecto_new):
        if not vuelos_trayecto_old:
            return True
        
        if len(vuelos_trayecto_old) != len(vuelos_trayecto_new):
            return True
        
        for v1, v2 in zip(vuelos_trayecto_old, vuelos_trayecto_new):
            if v1.id_vuelo != v2.id_vuelo or v1.duracion_enminutos != v2.duracion_enminutos:
                return True
            
        return False
        



    def procesar_flush(self, id_cliente):        
        logging.info(f'FLUSH Cliente: {id_cliente}')
        self._protocolo.finalizo_cliente(id_cliente)
        return None
        
    def procesar_finvuelo(self, id_cliente):
        # Recorrer todos los trayectos de vuelos_mas_rapidos
        self._fines_vuelo += 1
        if self._fines_vuelo < self._cant_filtros_escalas:
            return 'fin_vuelos'
        resultados = []
        logging.info(f"Procesando fin de vuelo cliente {id_cliente}")
        
        if id_cliente in self.vuelos_mas_rapido_cliente:
            vuelos_mas_rapido = self.vuelos_mas_rapido_cliente[id_cliente]
            for trayecto, vuelos in vuelos_mas_rapido.items():
                for vuelo in vuelos:
                    self.resultados_enviados += 1
                    if (self.resultados_enviados % 100) == 1:
                        logging.info(f'Enviando resultados: {self.resultados_enviados}')
                    id_vuelo = vuelo.id_vuelo            
                    duracion = vuelo.duracion
                    escalas = vuelo.escalas
                    resultado = ResultadoVuelosRapidos(id_vuelo, trayecto, escalas, duracion)
                    resultados.append(resultado)
                
            self._protocoloResultado.enviar_resultado_vuelos_rapidos(resultados, id_cliente)                
            logging.info(f'Resultados enviados: {self.resultados_enviados}')
            del self.vuelos_mas_rapido_cliente[id_cliente]
        self._protocoloResultado.enviar_fin_resultados_rapidos(id_cliente, self._id)
        
        self._protocolo.finalizo_cliente(id_cliente)
        return None
        
    def run(self):        
          logging.info("Iniciando filtro velocidad") 
          try:
            for id_cliente, linea in self._protocolo.recuperar_siguiente_linea():
                logging.debug(f'Recuperé la linea {linea} del cliente {id_cliente}')
                if linea[0] == 'fin_vuelos':
                    self._fines_vuelo += 1
                else:
                    self.vuelos_mas_rapido_cliente = self._recuperador_vuelos.recuperar_valores(self.vuelos_mas_rapido_cliente, id_cliente, linea)

            self._handle_protocolo_heartbeat = Process(target=self._protocolo_heartbeat.enviar_heartbeats)  
            self._handle_protocolo_heartbeat.start()
            self._protocolo.iniciar(self.procesar_vuelo, self.procesar_finvuelo, self.procesar_flush, self._id, self._cant_filtros_escalas) 
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
        if hasattr(self, '_protocoloResultado'):
            self._protocoloResultado.cerrar()

        if hasattr(self, '_protocolo_heartbeat'):
            self._protocolo_heartbeat.cerrar()

        if hasattr(self, '_handle_protocolo_heartbeat'):
            if self._handle_protocolo_heartbeat.is_alive():
                self._handle_protocolo_heartbeat.terminate()
            self._handle_protocolo_heartbeat.join()