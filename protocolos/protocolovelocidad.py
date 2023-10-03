from modelo.Aeropuerto import Aeropuerto
from modelo.Vuelo import Vuelo
from modelo.estado import Estado


class ProtocoloFiltroVelocidad:
    

    def __init__(self):
        self.corriendo = False
    
    def iniciar(self):        
        self.corriendo = False
        self._colas.crear_cola('cola')
        self._colas.consumir_mensajes('cola')
        
    def recibir_vuelo(self) -> (Vuelo, Estado):
        print()


    def enviar_vuelo(self,Vuelo: Vuelo):
        print()

    def enviar_fin_vuelos(self):
        print()