
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
FORMATO_FIN_VUELO = '!c32s'
IDENTIFICADOR_FLUSH = 'L'

ESTADO_FIN_VUELOS = 1
ESTADO_FIN_AEROPUERTOS = 1

STRING_ENCODING = 'utf-8'
FORMATO_MENSAJE_UNVUELO = '!32s3s3s50s8s'
NOMBRE_COLA = 'cola_escalas'



class ProtocoloFiltroEscalas(ProtocoloBase):
       
    def __init__(self, cant_filtros=None, id_cliente=None): 
       super().__init__()   
       self.TAMANO_VUELO = calcsize(FORMATO_MENSAJE_UNVUELO)
       self.nombre_cola = NOMBRE_COLA
       self._colas = ManejadorColas()
       self.corriendo = False
       if cant_filtros:
          self._cant_filtros= int(cant_filtros)        
       self.id_cliente = id_cliente
       
            
    def decodificar_vuelo(self, mensaje):                
        id_vuelo, origen, destino, escalas, duracion = unpack(FORMATO_MENSAJE_UNVUELO, mensaje)
        vuelo = Vuelo(id_vuelo.decode('utf-8'), origen.decode('utf-8'), destino.decode('utf-8'), 0, escalas.decode('utf-8').replace('\x00', ''), duracion.decode('utf-8'), 0)
        return vuelo


    def iniciar(self, procesar_vuelo, procesar_finvuelo,procesar_flush, id):
        self.corriendo = True
        self.procesar_vuelo = procesar_vuelo
        self.procesar_flush = procesar_flush
        self.procesar_finvuelo =  procesar_finvuelo
        self._colas.crear_cola_por_topico(self.nombre_cola)
        self._colas.consumir_mensajes_por_topico(self.nombre_cola, str(id), self.callback_function, id, post_ack_callback=self.borrar_archivos)
        self._colas.consumir()

        
        
    def traducir_vuelo(self, vuelo):
        return struct.pack(FORMATO_MENSAJE_UNVUELO.encode(STRING_ENCODING),
                                          vuelo.id_vuelo.encode(STRING_ENCODING),
                                          vuelo.origen.encode(STRING_ENCODING),
                                          vuelo.destino.encode(STRING_ENCODING),
                                          vuelo.escalas.encode(STRING_ENCODING),
                                          vuelo.duracion.encode(STRING_ENCODING)
                                          )


    def parar(self):        
        self._colas.dejar_de_consumir(NOMBRE_COLA)

    def cerrar(self):
        self._colas.cerrar()
        super().cerrar()
        
        