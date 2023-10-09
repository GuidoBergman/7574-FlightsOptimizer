from protocolofiltroescalas import ProtocoloFiltroEscalas
from protocolofiltroprecio import ProtocoloFiltroPrecio
from protocolofiltrodistancia import ProtocoloFiltroDistancia


class EnviadorFin:
    def __init__(self):
        self._protocolo_escalas = ProtocoloFiltroEscalas()        
        self._protocolo_precio = ProtocoloFiltroPrecio()
        self._protocolo_distancia = ProtocoloFiltroDistancia()

    def enviar_fin_vuelos(self):
        self._protocolo_escalas.enviar_fin_vuelos()
        self._protocolo_precio.enviar_fin_vuelos()
        self._protocolo_distancia.enviar_fin_vuelos()