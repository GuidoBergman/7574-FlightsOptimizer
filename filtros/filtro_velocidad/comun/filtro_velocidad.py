import logging
import signal
from manejador_colas import ManejadorColas
from modelo.Vuelo import Vuelo
from protocolovelocidad import ProtocoloFiltroVelocidad
from modelo.ResultadoVuelosRapidos import ResultadoVuelosRapidos
from protocolo_resultados_servidor import ProtocoloResultadosServidor

class FiltroVelocidad:
    def __init__(self, port, listen_backlog):
       self._colas = ManejadorColas('rabbitmq')
       self._protocolo = ProtocoloFiltroVelocidad()
       
       self._protocoloResultado = ProtocoloResultadosServidor()
       signal.signal(signal.SIGTERM, self.sigterm_handler)
       self.vuelos_mas_rapido = {}
       
        

    def sigterm_handler(self, _signo, _stack_frame):
        logging.info('action: sigterm_received')

        
    def procesar_vuelo(self, vuelo: Vuelo):
        
        
        # Concatenar origen y destino para obtener el proyecto
        trayecto = vuelo.origen + "-" + vuelo.destino
        # Obtener la duración del vuelo actual
        duracion_actual = vuelo.duracion
        logging.error(f"Procesando vuelo trayecto: { trayecto } de duracion { duracion_actual } ")
        
        # Comprobar si ya hay vuelos registrados para este proyecto
        if trayecto in self.vuelos_mas_rapido:
            # Obtener los vuelos actuales para este proyecto
            vuelos_proyecto = self.vuelos_mas_rapido[trayecto]

            # Ordenar los vuelos por duración (ascendente)
            vuelos_proyecto.sort(key=lambda x: x.duracion)

            # Verificar si la duración del vuelo actual es menor que la duración del vuelo más lento registrado
            if duracion_actual < vuelos_proyecto[-1].duracion:
                # Reemplazar el vuelo más lento con el vuelo actual
                vuelos_proyecto[-1] = vuelo

        else:
            # Si no hay vuelos registrados para este proyecto, crear una lista con el vuelo actual
            self.vuelos_mas_rapido[trayecto] = [vuelo]

        # Si hay más de 2 vuelos para este proyecto, mantener solo los 2 más rápidos
        if len(self.vuelos_mas_rapido[trayecto]) > 2:
            self.vuelos_mas_rapido[trayecto].sort(key=lambda x: x.duracion)
            self.vuelos_mas_rapido[trayecto] = self.vuelos_mas_rapido[trayecto][:2]


    def procesar_finvuelo(self):
        # Recorrer todos los trayectos de vuelos_mas_rapidos
        logging.info(f"INFO: Procesando fin de vuelo")
        logging.error(f"Procesando fin de vuelo")
        for trayecto, vuelos in self.vuelos_mas_rapido.items():
            for vuelo in vuelos:
                logging.error(f"Enviando trayecto: { trayecto }")
                id_vuelo = vuelo.id_vuelo            
                duracion = vuelo.duracion
                resultado = ResultadoVuelosRapidos(id_vuelo, trayecto, "", duracion)
                self._protocoloResultado.enviar_resultado_vuelos_rapidos(resultado)
        
    def run(self):        
          self._protocolo.iniciar(self.procesar_vuelo, self.procesar_finvuelo)              
          while self._protocolo.corriendo:
              a = 1
          return
        
        
        
           
