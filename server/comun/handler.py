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
        logging.error(f'CVant filtros precio (handler): {cant_filtros_precio}')
        self._protocoloPrecio = ProtocoloFiltroPrecio(cant_filtros_precio)
        

    def run(self, vuelos):
        while True:
            vuelo = vuelos.get()  
            if vuelo == EOF_MSG:
                break

            logging.info(f'Acción: recibir_vuelo | estado: OK | Vuelo recibido (handler):  id vuelo: {vuelo.id_vuelo}   origen: {vuelo.origen}   destino: {vuelo.destino}  precio: {vuelo.precio} distancia: {vuelo.distancia} duracion: {vuelo.duracion} escalas: {vuelo.escalas}')
            self._protocoloEscalas.enviar_vuelo(vuelo)
            self._protocoloDistancia.enviar_vuelo(vuelo)
            self._protocoloPrecio.enviar_vuelo(vuelo)
            