from protocolofiltroescalas import ProtocoloFiltroEscalas



class EnviadorFin:
    def __init__(self):
        self._protocolo_escalas = ProtocoloFiltroEscalas()

    def enviar_fin_vuelos(self):
        self._protocolo_escalas.enviar_fin_vuelos()