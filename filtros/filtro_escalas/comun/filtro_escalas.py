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
    def __init__(self):
       signal.signal(signal.SIGTERM, self.sigterm_handler)
       self._protocolo = ProtocoloFiltroEscalas()
       self._protocoloResultado = ProtocoloResultadosServidor()
       self._protocoloVelocidad = ProtocoloFiltroVelocidad()
       self.vuelos_con_tres_escalas = []
       self.corriendo = True
       
        
    def sigterm_handler(self, _signo, _stack_frame):
        self._protocolo.parar()
        logging.info('action: sigterm_received')

        
    def procesar_vuelo(self, vuelo: Vuelo):
        logging.error(f'Procesando el vuelo{ vuelo.id_vuelo } escalas { vuelo.escalas }')
        if len(vuelo.escalas.split("||")) >= 3:            
            logging.error(f'Envia como resultado el vuelo { vuelo.id_vuelo }')

            resultado = ResultadoFiltroEscalas(vuelo.id_vuelo, vuelo.origen + '-' + vuelo.destino,
                vuelo.precio, vuelo.escalas
            )
            self._protocoloResultado.enviar_resultado_filtro_escalas(resultado)
            self._protocoloVelocidad.enviar_vuelo(vuelo)

    def procesar_finvuelo(self):        
        logging.error(f'FIN DE VUELOS')
        self._protocoloVelocidad.enviar_fin_vuelos()

    def run(self):
          self._protocolo.iniciar(self.procesar_vuelo, self.procesar_finvuelo)
          