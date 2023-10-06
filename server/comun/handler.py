import logging
import signal

from protocolofiltroescalas import ProtocoloFiltroEscalas

EOF_MSG = 'EOF'

class Handler:
    def __init__(self):
        self._protocolo = ProtocoloFiltroEscalas()

    def run(self, vuelos):
        while True:
            vuelo = vuelos.get()  
            if vuelo == EOF_MSG:
                break

            logging.info(f'Acción: recibir_vuelo | estado: OK | Vuelo recibido (handler):  id vuelo: {vuelo.id_vuelo}   origen: {vuelo.origen}   destino: {vuelo.destino}  precio: {vuelo.precio} distancia: {vuelo.distancia} duracion: {vuelo.duracion} escalas: {vuelo.escalas}')
            self._protocolo.enviar_vuelo(vuelo)
            