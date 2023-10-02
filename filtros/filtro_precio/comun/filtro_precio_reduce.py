from filtros.filtro_precio.comun.resumen_precios import ResumenPrecios
from manejador_colas import ManejadorColas
from modelo.estado_vuelo import EstadoVuelo
from protocolofiltroprecio import ProtocoloFiltroPrecio
from protocoloresultados import ProtocoloResultado


class PrecioReduce:

    def __init__(self):        
        self.resumenes_precios = []
        self.corriendo = False
        self._colas = ManejadorColas('rabbitmq')
        self._protocolo = ProtocoloFiltroPrecio()
        self._protocoloResultado = ProtocoloResultado()


        
    def agregar_resumen(self, resumen: ResumenPrecios):
        self.resumenes_precios.append(resumen)
        

    def calcular_promedio(self):
        total_precios = sum([resumen.suma_precios for resumen in self.resumenes_precios])
        total_vuelos = sum([resumen.total_vuelos for resumen in self.resumenes_precios])
        return total_precios / total_vuelos if total_vuelos > 0 else 0.0

    def enviar_query(self):
        promedio = self.calcular_promedio()

        for resumen in self.resumenes_precios:
            if resumen.maximo_precio > promedio:
                estadisticas = ResumenPrecios(resumen.trayecto, resumen.maximo_precio, promedio)
                self._protocoloResultado.EnviartResultadoPrecios(estadisticas)

    def run(self):
          
          while self.corriendo:
            resumen, estado = self._protocolo.recibir_resumen()
            if estado == EstadoVuelo.OK:
                self.agregar_resumen(resumen)
            else:
                break
          self.enviar_query()

    