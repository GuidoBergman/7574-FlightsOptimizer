
import logging
from struct import unpack, pack, calcsize
import struct
from modelo.Vuelo import Vuelo
from manejador_colas import ManejadorColas
from modelo.Aeropuerto import Aeropuerto
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
NOMBRE_COLA = 'cola_escalas'
HOST_COLAS = 'rabbitmq'


class ProtocoloFiltroEscalas:
       
    def __init__(self):    
       self.nombre_cola = NOMBRE_COLA
       self._colas = ManejadorColas(HOST_COLAS)
       self.corriendo = False
    

    def callback_function(self, body):
        # procesar los mensajes, llamando a procesar_vuelo o procesar_finvuelo segun corresponda
        logging.debug(f'llego mensaje body: {body}')
        if body.decode('utf-8').startswith(IDENTIFICADOR_VUELO):
            self.procesar_vuelo(self.traducir_vuelo(body))
        else:
            self.procesar_finvuelo()

    def traducir_vuelo(self, mensaje):
        
        formato_mensaje = FORMATO_MENSAJE_VUELO
        tipomensaje, cantidad_vuelos, id_vuelo, origen, destino, escalas, duracion = unpack(formato_mensaje, mensaje)
        
        
        vuelo = Vuelo(id_vuelo.decode('utf-8'), origen.decode('utf-8'), destino.decode('utf-8'), 0, escalas.decode('utf-8').replace('\x00', ''), duracion.decode('utf-8'), 0)
        return vuelo


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
        
        