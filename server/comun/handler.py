import logging
import signal

from protocolofiltroescalas import ProtocoloFiltroEscalas
from protocolofiltrodistancia import ProtocoloFiltroDistancia
from protocolofiltroprecio import ProtocoloFiltroPrecio

EOF_MSG = 'EOF'

class Handler:
    def __init__(self, cant_filtros_precio):
        self._protocoloEscalas = ProtocoloFiltroEscalas()
        self._protocoloDistancia = ProtocoloFiltroDistancia()
        self._protocoloPrecio = ProtocoloFiltroPrecio(cant_filtros_precio)
        signal.signal(signal.SIGTERM, self._sigterm_handler)
        

    def run(self, vuelos):
        while True:
            try:
                vuelo = vuelos.get() 
            except EOFError:
                break
             
            if vuelo == EOF_MSG:
                break

            logging.debug(f'Acción: recibir_vuelo | estado: OK | Vuelo recibido (handler):  id vuelo: {vuelo.id_vuelo}   origen: {vuelo.origen}   destino: {vuelo.destino}  precio: {vuelo.precio} distancia: {vuelo.distancia} duracion: {vuelo.duracion} escalas: {vuelo.escalas}')
            self._protocoloEscalas.enviar_vuelo(vuelo)
            self._protocoloDistancia.enviar_vuelo(vuelo)
            self._protocoloPrecio.enviar_vuelo(vuelo)
            

    def _sigterm_handler(self, _signo, _stack_frame):
        logging.error('Sigterm recibida (Handler)')

        self._protocoloEscalas.cerrar()
        self._protocoloDistancia.cerrar()
        self._protocoloPrecio.cerrar()
            
