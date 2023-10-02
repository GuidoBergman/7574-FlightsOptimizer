
from modelo.ResultadoEstadisticasPrecios import ResultadoEstadisticaPrecios
from modelo.ResultadoFiltroDistancia import ResultadoFiltroDistancia
from modelo.ResultadoFiltroEscalas import ResultadoFiltroEscalas
from modelo.ResultadoVuelosRapidos import ResultadoVuelosRapidos


class ProtocoloResultado:
    def EnviarResultadoVuelosRapidos(resultado: ResultadoVuelosRapidos):
        print()

    def EnviarResultadoFiltroDistancia(resultado: ResultadoFiltroDistancia):
        print()
        
    def EnviarResultadoFiltroEscalas(resultado: ResultadoFiltroEscalas):
        print()

    def EnviarResultadoFiltroPrecio(resultado: ResultadoEstadisticaPrecios):
        print()
