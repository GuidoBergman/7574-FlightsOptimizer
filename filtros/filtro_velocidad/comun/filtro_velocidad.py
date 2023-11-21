import logging
import signal
import re
from manejador_colas import ManejadorColas
from modelo.Vuelo import Vuelo
from protocolovelocidad import ProtocoloFiltroVelocidad
from modelo.ResultadoVuelosRapidos import ResultadoVuelosRapidos
from protocolo_resultados_servidor import ProtocoloResultadosServidor


class FiltroVelocidad:
    def __init__(self, id, cant_filtros_escalas):
       self._protocolo = ProtocoloFiltroVelocidad()       
       signal.signal(signal.SIGTERM, self.sigterm_handler)
       self.vuelos_mas_rapido_cliente = {}
       self._fines_vuelo = 0
        
       self._id = id
       self._cant_filtros_escalas = cant_filtros_escalas
       self.vuelos_procesados = 0       
       self.resultados_enviados = 0
 

        
    def calcular_minutos(self, duracion_str):
        minutos_totales = 0
        
        horas = 0
        minutos = 0

        match = re.match(r'PT(\d+)H?(\d*)M?$', duracion_str)            
        if match:
            horas = int(match.group(1))
            minutos = int(match.group(2)) if match.group(2) else 0
            minutos_totales += horas * 60 + minutos
        return minutos_totales

    def procesar_vuelo(self, id_cliente, vuelos):
        
        self.vuelos_procesados += 1
        if (self.vuelos_procesados % 300) == 1:
            logging.info(f'Procesando Vuelo: {self.vuelos_procesados}')
            
        if id_cliente in self.vuelos_mas_rapido_cliente:
            vuelos_mas_rapido = self.vuelos_mas_rapido_cliente[id_cliente]
        else:
            vuelos_mas_rapido = {}
            
        for vuelo in vuelos:
            trayecto = vuelo.origen + "-" + vuelo.destino
            # Obtener la duración del vuelo actual
            vuelo.duracion_enminutos = self.calcular_minutos(vuelo.duracion)
            logging.debug(f"Procesando vuelo trayecto: { trayecto } de duracion: {vuelo.duracion} en minutos: { vuelo.duracion_enminutos } ")
            # Comprobar si ya hay vuelos registrados para este trayecto
            if trayecto in vuelos_mas_rapido:
                # Agregar el vuelo a la lista
                vuelos_mas_rapido[trayecto].append(vuelo)
            else:
                # Si no hay vuelos registrados para este proyecto, crear una lista con el vuelo actual
                vuelos_mas_rapido[trayecto] = [vuelo]

            # Si hay más de 2 vuelos para este proyecto, mantener solo los 2 más rápidos
            if len(vuelos_mas_rapido[trayecto]) > 2:
                vuelos_mas_rapido[trayecto].sort(key=lambda x: x.duracion_enminutos)
                vuelos_mas_rapido[trayecto] = vuelos_mas_rapido[trayecto][:2]
          
        self.vuelos_mas_rapido_cliente[id_cliente] = vuelos_mas_rapido

    def procesar_finvuelo(self, id_cliente):
        # Recorrer todos los trayectos de vuelos_mas_rapidos
        self._fines_vuelo += 1
        if self._fines_vuelo < self._cant_filtros_escalas:
            return
     
        logging.info(f"Procesando fin de vuelo")        
        vuelos_mas_rapido = self.vuelos_mas_rapido_cliente[id_cliente]
        self._protocoloResultado = ProtocoloResultadosServidor()
        for trayecto, vuelos in vuelos_mas_rapido.items():
            for vuelo in vuelos:
                self.resultados_enviados += 1
                if (self.resultados_enviados % 100) == 1:
                    logging.info(f'Enviando resultados: {self.resultados_enviados}')
                id_vuelo = vuelo.id_vuelo            
                duracion = vuelo.duracion
                escalas = vuelo.escalas
                resultado = ResultadoVuelosRapidos(id_vuelo, trayecto, escalas, duracion)
                self._protocoloResultado.enviar_resultado_vuelos_rapidos(resultado, id_cliente)
        logging.info(f'Resultados enviados: {self.resultados_enviados}')
        del self.vuelos_mas_rapido_cliente[id_cliente]
        self._protocoloResultado.enviar_fin_resultados_rapidos(id_cliente)
        
    def run(self):        
          logging.info("Iniciando filtro velocidad") 
          self._protocolo.iniciar(self.procesar_vuelo, self.procesar_finvuelo, self._id, self._cant_filtros_escalas) 

 
           
    def sigterm_handler(self, _signo, _stack_frame):
        logging.info('SIGTERM recibida')
        self._protocolo.cerrar()
        if self._protocoloResultado:
            self._protocoloResultado.cerrar()