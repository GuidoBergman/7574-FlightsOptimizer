
from manejador_colas import ManejadorColas
from modelo.ResultadoEstadisticasPrecios import ResultadoEstadisticaPrecios
from modelo.ResultadoFiltroDistancia import ResultadoFiltroDistancia
from modelo.ResultadoFiltroEscalas import ResultadoFiltroEscalas
from modelo.ResultadoVuelosRapidos import ResultadoVuelosRapidos
from modelo.Vuelo import Vuelo

HOST_COLAS = 'rabbitmq'
COLA_RESULTADOS = 'cola_resultados'

class ProtocoloResultadosServidor:

    def __init__(self):
       self._colas = ManejadorColas(HOST_COLAS)
       self.corriendo = False
    
    def iniciar(self):        
        self.corriendo = False
        self._colas.crear_cola(COLA_RESULTADOS)
        self._colas.consumir_mensajes(COLA_RESULTADOS)

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
