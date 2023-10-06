
from manejador_colas import ManejadorColas
from modelo.ResultadoEstadisticasPrecios import ResultadoEstadisticaPrecios
from modelo.ResultadoFiltroDistancia import ResultadoFiltroDistancia
from modelo.ResultadoFiltroEscalas import ResultadoFiltroEscalas
from modelo.ResultadoVuelosRapidos import ResultadoVuelosRapidos
from modelo.Vuelo import Vuelo


class ProtocoloResultadosServidor:

    def __init__(self):
       self._colas = ManejadorColas('rabbitmq')
       self.corriendo = False
    
    def iniciar(self):        
        self.corriendo = False
        self._colas.crear_cola('cola')
        self._colas.consumir_mensajes('cola')

    def parar(self):        
        self.corriendo = False

    def enviar_resultado_vuelos_rapidos(resultado: ResultadoVuelosRapidos):
        print()

    def enviar_resultado_filtro_distancia(resultado: ResultadoFiltroDistancia):
        print()
        
    def enviar_resultado_filtro_escalas(self, resultado: ResultadoFiltroEscalas):
        print()

    def enviar_resultado_filtro_precio(resultado: ResultadoEstadisticaPrecios):
        print()
