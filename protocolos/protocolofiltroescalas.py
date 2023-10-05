
import logging
from struct import unpack, pack, calcsize
import struct
from manejador_colas import ManejadorColas
from modelo.Aeropuerto import Aeropuerto
from modelo.Vuelo import Vuelo
from modelo.estado import Estado


TAMANIO_IDENTIFICADOR_MENSAJE = 1
IDENTIFICADOR_VUELO = 'V'
IDENTIFICADOR_AEROPUERTO = 'A'
IDENTIFICADOR_FIN_VUELO = 'F'
IDENTIFICADOR_FIN_AEROPUERTO = 'E'

ESTADO_FIN_VUELOS = 1
ESTADO_FIN_AEROPUERTOS = 1

STRING_ENCODING = 'utf-8'
FORMATO_MENSAJE_VUELO = '!cH32s3s3s50s8s'

class ProtocoloFiltroEscalas:
       
    def __init__(self):    
       self.nombre_cola = 'cola'
       self._colas = ManejadorColas('rabbitmq')
       self.corriendo = False
    

    def callback_function(self, body):
        # procesar los mensajes, llamando a procesar_vuelo o procesar_finvuelo segun corresponda
        logging.error(f'llego mensaje body:   {body}')
        #self.procesar_vuelo(self.traducir_vuelo(body))

    def traducir_vuelo(self):
        
        formato_mensaje = FORMATO_MENSAJE_VUELO
        tamanio_mensaje = calcsize(formato_mensaje)
        estado, mensaje, _ = self._socket.receive(tamanio_mensaje)
        cantidad_vuelos, id, origen, destino, escalas, distancia = unpack(formato_mensaje, mensaje)

        


    def iniciar(self, procesar_vuelo, procesar_finvuelo):
        self.corriendo = True
        self.procesar_vuelo = procesar_vuelo
        self.procesar_finvuelo =  procesar_finvuelo
        self._colas.crear_cola(self.nombre_cola)
        self._colas.consumir_mensajes(self.nombre_cola, self.callback_function)



    def enviar_vuelo(self, vuelo):
        tipo_mensaje = IDENTIFICADOR_VUELO.encode(STRING_ENCODING)
        tamanio_batch = 1
        
        mensaje_empaquetado = struct.pack(FORMATO_MENSAJE_VUELO,
                                      tipo_mensaje,
                                      tamanio_batch,
                                      vuelo.id_vuelo.encode(STRING_ENCODING),
                                      vuelo.origen.encode(STRING_ENCODING),
                                      vuelo.destino.encode(STRING_ENCODING),
                                      vuelo.escalas.encode(STRING_ENCODING),
                                      vuelo.duracion.encode(STRING_ENCODING)
                                      )
    
        
        self._colas.enviar_mensaje(self.nombre_cola, mensaje_empaquetado)



    def enviar_fin_vuelos(self):
        self._socket.send(IDENTIFICADOR_FIN_VUELO.encode(STRING_ENCODING), TAMANIO_IDENTIFICADOR_MENSAJE)



    def parar(self):        
        self.corriendo = False
        
        