import logging
import signal

from manejador_colas import ManejadorColas
from modelo.estado import Estado
from protocolofiltroescalas import ProtocoloFiltroEscalas
from modelo.Vuelo import Vuelo
from protocolo_resultados_servidor import ProtocoloResultadosServidor
from protocolovelocidad import ProtocoloFiltroVelocidad
from modelo.ResultadoFiltroEscalas import ResultadoFiltroEscalas


class FiltroEscalas:
    def __init__(self, id, cant_filtros_velocidad):
       self._protocolo = ProtocoloFiltroEscalas()
       self._protocoloResultado = ProtocoloResultadosServidor()
       self._protocoloVelocidad = ProtocoloFiltroVelocidad(cant_filtros_velocidad)
       signal.signal(signal.SIGTERM, self.sigterm_handler)
       self.vuelos_con_tres_escalas = []
       self.vuelos_procesados = 0
       self._id = id
       
        
    def procesar_vuelo(self, id_cliente, vuelos):
        self.vuelos_procesados += 1;
        if (self.vuelos_procesados % 300) == 1:
            logging.info(f'Procesando Vuelo: {self.vuelos_procesados}')  
        
        for vuelo in vuelos:
            logging.debug(f'Procesando el vuelo{ vuelo.id_vuelo } escalas { vuelo.escalas }')
            if len(vuelo.escalas.split("||")[:-1]) >= 3:
                logging.debug(f'Envia como resultado el vuelo { vuelo.id_vuelo }')
                resultado = ResultadoFiltroEscalas(vuelo.id_vuelo, vuelo.origen + '-' + vuelo.destino,
                    vuelo.precio, vuelo.escalas
                )
                self._protocoloResultado.enviar_resultado_filtro_escalas(resultado, id_cliente)
                #self._protocoloVelocidad.enviar_vuelo(vuelo)

    def procesar_finvuelo(self, id_cliente):        
        logging.info(f'Fin de vuelos Cliente: {id_cliente}')
        #self._protocoloVelocidad.enviar_fin_vuelos()
        self._protocoloResultado.enviar_fin_resultados_escalas(id_cliente)

    def run(self):
          logging.info('Iniciando filtro escalas')  
          self._protocolo.iniciar(self.procesar_vuelo, self.procesar_finvuelo)
          
    
    def sigterm_handler(self, _signo, _stack_frame):
        logging.info('SIGTERM recibida')
        self._protocolo.cerrar()
        self._protocoloResultado.cerrar()
        self._protocoloVelocidad.cerrar()