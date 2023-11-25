import logging
import signal

from manejador_colas import ManejadorColas
from modelo.estado import Estado
from protocolofiltroescalas import ProtocoloFiltroEscalas
from modelo.Vuelo import Vuelo
from protocolo_resultados_servidor import ProtocoloResultadosServidor
from protocolovelocidad import ProtocoloFiltroVelocidad
from modelo.ResultadoFiltroEscalas import ResultadoFiltroEscalas

from multiprocessing import Process
from protocolo_enviar_heartbeat import ProtocoloEnviarHeartbeat, IDENTIFICADOR_FILTRO_ESCALAS
from socket_comun_udp import SocketComunUDP


class FiltroEscalas:
    def __init__(self, id, cant_filtros_velocidad, cant_watchdogs, periodo_heartbeat, host_watchdog, port_watchdog):
       self._protocolo = ProtocoloFiltroEscalas()
       self._protocoloResultado = ProtocoloResultadosServidor()
       self._protocoloVelocidad = ProtocoloFiltroVelocidad(cant_filtros_velocidad)
       signal.signal(signal.SIGTERM, self.sigterm_handler)
       self.vuelos_con_tres_escalas = []
       self.vuelos_procesados = 0
       self._id = id
       
       socket = SocketComunUDP()
       self._protocolo_heartbeat = ProtocoloEnviarHeartbeat(socket, host_watchdog, port_watchdog, cant_watchdogs,
        IDENTIFICADOR_FILTRO_ESCALAS, periodo_heartbeat, id)
        
    def procesar_vuelo(self, id_cliente, vuelos):
        resultados = []
        self.vuelos_procesados += 1;
        if (self.vuelos_procesados % 300) == 1:
            logging.info(f'Procesando Vuelo: {self.vuelos_procesados}')  

        vuelos_paravelocidad = []        
        for vuelo in vuelos:
            logging.debug(f'Procesando el vuelo{ vuelo.id_vuelo } escalas { vuelo.escalas }')
            if len(vuelo.escalas.split("||")[:-1]) >= 3:
                logging.debug(f'Envia como resultado el vuelo { vuelo.id_vuelo }')
                resultado = ResultadoFiltroEscalas(vuelo.id_vuelo, vuelo.origen + '-' + vuelo.destino,
                    vuelo.precio, vuelo.escalas
                )
                resultados.append(resultado)
                vuelos_paravelocidad.append(vuelo)
        self._protocoloResultado.enviar_resultado_filtro_escalas(resultados, id_cliente)
        self._protocoloVelocidad.enviar_vuelos(id_cliente, vuelos_paravelocidad)

    def procesar_finvuelo(self, id_cliente):        
        logging.info(f'Fin de vuelos Cliente: {id_cliente}')
        self._protocoloVelocidad.enviar_fin_vuelos(id_cliente)
        self._protocoloResultado.enviar_fin_resultados_escalas(id_cliente)

    def run(self):
          logging.info('Iniciando filtro escalas')  
          try:
            self._handle_protocolo_heartbeat = Process(target=self._protocolo_heartbeat.enviar_heartbeats)  
            self._handle_protocolo_heartbeat.start()
            self._protocolo.iniciar(self.procesar_vuelo, self.procesar_finvuelo)
          except Exception as e:
            logging.error(f'Ocurrió una excepción: {e}')
            self.cerrar()
          
    
    def sigterm_handler(self, _signo, _stack_frame):
        logging.error('SIGTERM recibida')
        self.cerrar()
        

    def cerrar(self):
        logging.error('Cerrando recursos')
        self._protocolo.cerrar()
        self._protocoloResultado.cerrar()
        self._protocoloVelocidad.cerrar()

        if hasattr(self, '_protocolo_heartbeat'):
            self._protocolo_heartbeat.cerrar()

        if hasattr(self, '_handle_protocolo_heartbeat'):
            if self._handle_protocolo_heartbeat.is_alive():
                self._handle_protocolo_heartbeat.terminate()
            self._handle_protocolo_heartbeat.join()