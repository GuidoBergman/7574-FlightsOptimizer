import logging
import signal
from manejador_colas import ManejadorColas
from modelo.Vuelo import Vuelo
from protocolovelocidad import ProtocoloFiltroVelocidad
from modelo.ResultadoVuelosRapidos import ResultadoVuelosRapidos
from protocolo_resultados_servidor import ProtocoloResultadosServidor

class FiltroVelocidad:
    def __init__(self, id):
       self._colas = ManejadorColas('rabbitmq')
       self._protocolo = ProtocoloFiltroVelocidad()
       
       self._protocoloResultado = ProtocoloResultadosServidor()
       signal.signal(signal.SIGTERM, self.sigterm_handler)
       self.vuelos_mas_rapido = {}

       self._id = id
       
        

    def sigterm_handler(self, _signo, _stack_frame):
        logging.info('action: sigterm_received')

        
    def procesar_vuelo(self, vuelo: Vuelo):
        
        
        # Concatenar origen y destino para obtener el tryecto
        trayecto = vuelo.origen + "-" + vuelo.destino
        # Obtener la duración del vuelo actual
        duracion_actual = vuelo.duracion
        logging.error(f"Procesando vuelo trayecto: { trayecto } de duracion { duracion_actual } ")
        
        # Comprobar si ya hay vuelos registrados para este trayecto
        if trayecto in self.vuelos_mas_rapido:
            # Agregar el vuelo a la lista
            self.vuelos_mas_rapido[trayecto].append(vuelo)

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
                escalas = vuelo.escalas
                resultado = ResultadoVuelosRapidos(id_vuelo, trayecto, escalas, duracion)
                self._protocoloResultado.enviar_resultado_vuelos_rapidos(resultado)
        
    def run(self):        
          self._protocolo.iniciar(self.procesar_vuelo, self.procesar_finvuelo, self._id)              
          
        
        
           
