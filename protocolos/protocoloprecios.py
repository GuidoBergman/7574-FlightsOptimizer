import string
from filtros.filtro_precio.comun import resumen_precios
from manejador_colas import ManejadorColas
from modelo.Vuelo import Vuelo
from modelo.estado import Estado


class ProtocoloPrecio:

    def __init__(self):    
       self._colas = ManejadorColas('rabbitmq')
       self.corriendo = False
    

    def callback_function(self, body):
        # procesar los mensajes, llamando a procesar_vuelo o procesar_finvuelo segun corresponda
        self.procesar_vuelo()


    def iniciar(self, procesar_vuelo, procesar_finvuelo):
        self.corriendo = True
        self.procesar_vuelo = procesar_vuelo
        self.procesar_finvuelo =  procesar_finvuelo
        self._colas.crear_cola('cola')
        self._colas.consumir_mensajes('cola', self.callback_function)

    def parar(self):        
        self.corriendo = False
        
        
