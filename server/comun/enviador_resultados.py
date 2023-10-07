from protocolo_resultados_cliente import ProtocoloResultadosCliente
from modelo.ResultadoEstadisticasPrecios import ResultadoEstadisticaPrecios
from modelo.ResultadoFiltroDistancia import ResultadoFiltroDistancia
from modelo.ResultadoFiltroEscalas import ResultadoFiltroEscalas
from modelo.ResultadoVuelosRapidos import ResultadoVuelosRapidos
import logging

class EnviadorResultados:
  def __init__(self, socket):
    self._protocolo = ProtocoloResultadosCliente(socket)

  

  def enviar_resultados(self):
    resultado = ResultadoVuelosRapidos('12345678901234567890123456789012', 'DEM-BAZ', 'ALTA ESCALA', 'PT1H2M')
    self._protocolo.enviar_resultado_vuelos_rapidos(resultado)

    self._protocolo.enviar_fin_resultados_rapidos()
    
    resultado = ResultadoFiltroEscalas('12345678901234567890123456789012',
    'BUE-MAN', 1.1, 'Trank')
    self._protocolo.enviar_resultado_filtro_escalas(resultado)

    resultado = ResultadoEstadisticaPrecios('BUE-MAR', 10, 5.1)
    self._protocolo.enviar_resultado_filtro_precio(resultado)

    self._protocolo.enviar_fin_resultados_escalas()

    resultado = ResultadoFiltroDistancia('12345678901234567890123456789012', 'MAR-DEL', 3)
    self._protocolo.enviar_resultado_filtro_distancia(resultado)

    self._protocolo.enviar_fin_resultados_distancia()
    self._protocolo.enviar_fin_resultados_filtro_precio()

         

  def cerrar(self):
        self._protocolo.cerrar()
         