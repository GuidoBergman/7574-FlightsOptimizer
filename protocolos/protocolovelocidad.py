import logging
from struct import unpack, pack, calcsize
import struct
from manejador_colas import ManejadorColas
from modelo.Aeropuerto import Aeropuerto
from modelo.Vuelo import Vuelo
from modelo.estado import Estado
from protocolobase import ProtocoloBase, FORMATO_FIN

TAMANIO_IDENTIFICADOR_MENSAJE = 1
IDENTIFICADOR_VUELO = 'V'
IDENTIFICADOR_AEROPUERTO = 'A'
IDENTIFICADOR_FIN_VUELO = 'F'
IDENTIFICADOR_FIN_AEROPUERTO = 'E'
IDENTIFICADOR_FLUSH = 'L'

ESTADO_FIN_VUELOS = 1
ESTADO_FIN_AEROPUERTOS = 1

STRING_ENCODING = 'utf-8'
FORMATO_MENSAJE_UNVUELO = '!32s3s3s8s'
FORMATO_FIN_VUELO = '!c32s'


class ProtocoloFiltroVelocidad(ProtocoloBase):

    def __init__(self, cant_filtros=None, id_cliente=None): 
       super().__init__()   
       self.TAMANO_VUELO = calcsize(FORMATO_MENSAJE_UNVUELO)
       self._colas = ManejadorColas()
       self.corriendo = False
       self.nombre_cola = 'cola_FiltroVelocidad'       
       if cant_filtros:
          self._cant_filtros= int(cant_filtros)        
       self.id_cliente = id_cliente
       
    def decodificar_vuelo(self, mensaje):        
        id_vuelo, origen, destino, duracion = unpack(FORMATO_MENSAJE_UNVUELO, mensaje)
        vuelo = Vuelo(id_vuelo.decode('utf-8'), origen.decode('utf-8'), destino.decode('utf-8'), 0, "", duracion.decode('utf-8'), 0)
        return vuelo


    def iniciar(self, procesar_vuelo, procesar_finvuelo, procesar_flush,id, cant_filtros_escalas):
        self.corriendo = True
        self.procesar_vuelo = procesar_vuelo
        self.procesar_finvuelo =  procesar_finvuelo
        self.procesar_flush = procesar_flush
        self._cant_filtros_escalas = cant_filtros_escalas
        self._colas.crear_cola_por_topico(self.nombre_cola)
        self._colas.consumir_mensajes_por_topico(self.nombre_cola, self.callback_function, id, post_ack_callback=self.borrar_archivos)
        self._colas.consumir()

    def traducir_vuelo(self, vuelo):
        return struct.pack(FORMATO_MENSAJE_UNVUELO,
                                      vuelo.id_vuelo.encode(STRING_ENCODING),
                                      vuelo.origen.encode(STRING_ENCODING),
                                      vuelo.destino.encode(STRING_ENCODING),
                                      vuelo.duracion.encode(STRING_ENCODING))


    # Aqui se envia el id del filtro que manda (en vez de uno por defecto)
    def enviar_fin_vuelos(self, id_cliente, id_enviador):
        logging.info(f"ENVIA FIN VUELO {id_cliente }  id enviador: {id_enviador}")
        for i in range(1, self._cant_filtros + 1):
            mensaje = pack(FORMATO_FIN, IDENTIFICADOR_FIN_VUELO.encode(STRING_ENCODING), id_cliente.encode(STRING_ENCODING), id_enviador)
            logging.debug(f'Body mensaje fin de vuelo enviado: {mensaje}')
            self._colas.enviar_mensaje_por_topico(self.nombre_cola,mensaje, i)


    def parar(self):        
        self.corriendo = False
        
    def cerrar(self):
        self._colas.cerrar()
        super().cerrar()