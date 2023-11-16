from protocolofiltroescalas import ProtocoloFiltroEscalas
from protocolofiltroprecio import ProtocoloFiltroPrecio
from protocolofiltrodistancia import ProtocoloFiltroDistancia


class EnviadorFin:
    def __init__(self, cant_filtros_escalas,
    cant_filtros_distancia, cant_filtros_precio):
        self._protocolo_escalas = ProtocoloFiltroEscalas()        
        self._protocolo_precio = ProtocoloFiltroPrecio(cant_filtros_precio)
        self._protocolo_distancia = ProtocoloFiltroDistancia()
        
        self._cant_filtros_escalas = cant_filtros_escalas
        self._cant_filtros_distancia = cant_filtros_distancia
        self._cant_filtros_precio = cant_filtros_precio

    def enviar_fin_vuelos(self, id_cliente):
        for i in range(self._cant_filtros_escalas):
            self._protocolo_escalas.enviar_fin_vuelos(id_cliente)

        for i in range(self._cant_filtros_precio):
            self._protocolo_precio.enviar_fin_vuelos(id_cliente)

        for i in range(self._cant_filtros_distancia):
            self._protocolo_distancia.enviar_fin_vuelos(id_cliente)