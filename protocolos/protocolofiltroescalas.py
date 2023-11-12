
import logging
from struct import unpack, pack, calcsize
import struct
from modelo.Vuelo import Vuelo
from manejador_colas import ManejadorColas
from modelo.Aeropuerto import Aeropuerto
from modelo.estado import Estado
from protocolo_cliente import FORMATO_MENSAJE_UNVUELO, FORMATO_MENSAJE_VUELO
from protocolobase import ProtocoloBase
import uuid


IDENTIFICADOR_VUELO = 'V'
IDENTIFICADOR_AEROPUERTO = 'A'
IDENTIFICADOR_FIN_VUELO = 'F'
IDENTIFICADOR_FIN_AEROPUERTO = 'E'
FORMATO_TOTAL_VUELOS =  '!H'

ESTADO_FIN_VUELOS = 1
ESTADO_FIN_AEROPUERTOS = 1

STRING_ENCODING = 'utf-8'
FORMATO_MENSAJE_UNVUELO = '!32s3s3s50s8s'
NOMBRE_COLA = 'cola_escalas'



class ProtocoloFiltroEscalas(ProtocoloBase):
       
    def __init__(self, id_cliente=None):    
       self.TAMANO_VUELO = calcsize(FORMATO_MENSAJE_UNVUELO)
       self.nombre_cola = NOMBRE_COLA
       self._colas = ManejadorColas()
       self.corriendo = False
       self.id_cliente = id_cliente
    

    def callback_function(self, body):
        # procesar los mensajes, llamando a procesar_vuelo o procesar_finvuelo segun corresponda
        logging.debug(f'llego mensaje body: {body}')
        if body.decode('utf-8').startswith(IDENTIFICADOR_VUELO):
            
            self.procesar_vuelo(self.decodificar_vuelos(body))
        else:
            self.procesar_finvuelo()

    def decodificar_vuelo(self, mensaje):        
        
        id_vuelo, origen, destino, escalas, duracion = unpack(FORMATO_MENSAJE_UNVUELO, mensaje)
        vuelo = Vuelo(id_vuelo.decode('utf-8'), origen.decode('utf-8'), destino.decode('utf-8'), 0, escalas.decode('utf-8').replace('\x00', ''), duracion.decode('utf-8'), 0)
        return vuelo


    def iniciar(self, procesar_vuelo, procesar_finvuelo):
        self.corriendo = True
        self.procesar_vuelo = procesar_vuelo
        self.procesar_finvuelo =  procesar_finvuelo
        self._colas.crear_cola(self.nombre_cola)
        self._colas.consumir_mensajes(self.nombre_cola, self.callback_function)
        self._colas.consumir()
        
    def traducir_vuelo(self, vuelo):
        return struct.pack(FORMATO_MENSAJE_UNVUELO,
                                          vuelo.id_vuelo.encode(STRING_ENCODING),
                                          vuelo.origen.encode(STRING_ENCODING),
                                          vuelo.destino.encode(STRING_ENCODING),
                                          vuelo.escalas.encode(STRING_ENCODING),
                                          vuelo.duracion.encode(STRING_ENCODING)
                                          )



    def enviar_vuelos(self, vuelos):
        self._colas.enviar_mensaje(self.nombre_cola, self.traducir_vuelos(self.id_cliente, vuelos))
        


    def enviar_fin_vuelos(self):
        self._colas.enviar_mensaje(self.nombre_cola, IDENTIFICADOR_FIN_VUELO.encode(STRING_ENCODING))



    def parar(self):        
        self._colas.dejar_de_consumir(NOMBRE_COLA)

    def cerrar(self):
        self._colas.cerrar()
        
        