import logging
import signal
from filtros.modelo.Vuelo import Vuelo
from manejador_colas import ManejadorColas
from protocolovelocidad import ProtocoloFiltroVelocidad

class FiltroVelocidad:
    def __init__(self, port, listen_backlog):
       self._colas = ManejadorColas('rabbitmq')
       self._protocolo = ProtocoloFiltroVelocidad()

       signal.signal(signal.SIGTERM, self.sigterm_handler)
       self.vuelos_con_tres_escalas = []
       
        
    def mandar_resultado(self, vuelo: Vuelo):
        # Lógica para manejar el resultado, por ejemplo, imprimirlo o enviarlo a otra parte.
        print(f"Vuelo con más de 3 escalas: {vuelo.trayecto}, ID: {vuelo.id_vuelo}")

    def mandar_resultado(self, vuelo: Vuelo):
        # Lógica para manejar el resultado, por ejemplo, imprimirlo o enviarlo a otra parte.
        print(f"Vuelo con más de 3 escalas: {vuelo.trayecto}, ID: {vuelo.id_vuelo}")

        

    def sigterm_handler(self, _signo, _stack_frame):
        logging.info('action: sigterm_received')

        
    def procesar_vuelo(self, vuelo: Vuelo):
        # Actualizar los 2 vuelos más rápidos por trayecto para vuelos con más de 3 escalas
        trayecto = vuelo.trayecto
        duracion_actual = vuelo.escalas[0]  # Supongamos que la duración es el primer segmento

        if trayecto in self.vuelos_mas_rapidos:
            vuelos_actuales = self.vuelos_mas_rapidos[trayecto]
            vuelos_actuales.append((vuelo.id_vuelo, duracion_actual))
            vuelos_actuales.sort(key=lambda x: x[1])
            if len(vuelos_actuales) > 2:
                vuelos_actuales.pop()
            self.vuelos_mas_rapidos[trayecto] = vuelos_actuales
        else:
            self.vuelos_mas_rapidos[trayecto] = [(vuelo.id_vuelo, duracion_actual)]

    def procesar_finvuelo(self):
        # Recorrer todos los trayectos de vuelos_mas_rapidos
        for trayecto, vuelos in self.vuelos_mas_rapidos.items():
            for vuelo_id, vuelo in vuelos:
                # Buscar el vuelo en base al ID
                self._protocolo.enviar_vuelo
        
    def run(self):
          self._colas.crear_cola('cola')
          self._colas.consumir_mensajes('cola')

            
          while self.corriendo:
            vuelo, estado = self._protocolo.recibir_vuelo()
            if estado == EstadoVuelo.OK:
                self.procesar_vuelo(vuelo)
            else:
                break
    
        
        
        
           
