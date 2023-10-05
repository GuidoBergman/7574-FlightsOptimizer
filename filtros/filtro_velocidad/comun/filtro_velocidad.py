import logging
import signal
from manejador_colas import ManejadorColas
from modelo.Vuelo import Vuelo
from protocolovelocidad import ProtocoloFiltroVelocidad
from protocoloresultados import ProtocoloResultado

class FiltroVelocidad:
    def __init__(self, port, listen_backlog):
       self._colas = ManejadorColas('rabbitmq')
       self._protocolo = ProtocoloFiltroVelocidad()
       
       self._protocoloResultado = ProtocoloResultado()
       signal.signal(signal.SIGTERM, self.sigterm_handler)
       self.vuelos_mas_rapido = []
       
        

    def sigterm_handler(self, _signo, _stack_frame):
        logging.info('action: sigterm_received')

        
    def procesar_vuelo(self, vuelo: Vuelo):
        # Concatenar origen y destino para obtener el proyecto
        trayecto = vuelo.origen + "-" + vuelo.destino

        # Obtener la duración del vuelo actual
        duracion_actual = vuelo.duracion

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
        for trayecto, vuelos in self.vuelos_mas_rapido.items():
            self._protocolo.enviar_vuelo(trayecto, vuelos)
        
    def run(self):
        
          self._protocolo.iniciar(self.procesar_vuelo, self.procesar_finvuelo)  
            
          while self._protocolo.corriendo:
              a = 1
          return
        
        
        
           
