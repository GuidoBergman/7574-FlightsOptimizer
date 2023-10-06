from protocolo_resultados_cliente import ProtocoloResultadosCliente
from modelo.ResultadoFiltroEscalas import ResultadoFiltroEscalas
import logging

class EnviadorResultados:
      def __init__(self, socket):
        self._protocolo = ProtocoloResultadosCliente(socket)

      def enviar_resultados(self):
         resultado = ResultadoFiltroEscalas('12345678901234567890123456789012',
         'BUE-MAN', 1.1, 'Trank')


         self._protocolo.enviar_resultado_filtro_escalas(resultado)

      def cerrar(self):
        self._protocolo.cerrar()
         