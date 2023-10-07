from protocolofiltroescalas import ProtocoloFiltroEscalas
from protocolofiltroprecio import ProtocoloFiltroPrecio


class EnviadorFin:
    def __init__(self):
        self._protocolo_escalas = ProtocoloFiltroEscalas()        
        self._protocolo_precio = ProtocoloFiltroPrecio()

    def enviar_fin_vuelos(self):
        self._protocolo_escalas.enviar_fin_vuelos()
        self._protocolo_precio.enviar_fin_vuelos()