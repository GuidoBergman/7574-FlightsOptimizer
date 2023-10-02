import string
from filtros.filtro_precio.comun import resumen_precios
from modelo.Vuelo import Vuelo
from modelo.estado_vuelo import EstadoVuelo


class ProtocoloFiltroPrecio:

    def recibir_vuelo(self) -> (Vuelo, EstadoVuelo):
        print()


    def enviar_vuelo(self,Vuelo, EstadoVuelo):
        print()


    def mandar_resumen(self, trayecto, resumen: resumen_precios):
        2

    def recibir_resumen(self) -> (string, resumen_precios, EstadoVuelo):
        2