import logging
from struct import unpack, pack, calcsize
import struct
from modelo.Vuelo import Vuelo
from manejador_colas import ManejadorColas
from modelo.Aeropuerto import Aeropuerto
from protocolobase import ProtocoloBase


TAMANIO_IDENTIFICADOR_MENSAJE = 1
IDENTIFICADOR_VUELO = 'V'
IDENTIFICADOR_AEROPUERTO = 'A'
IDENTIFICADOR_FIN_VUELO = 'F'
IDENTIFICADOR_FIN_AEROPUERTO = 'E'

IDENTIFICADOR_PROMEDIO = 'P'
IDENTIFICADOR_PROMEDIOGENERAL = 'G'
ESTADO_FIN_VUELOS = 1
ESTADO_FIN_AEROPUERTOS = 1
STRING_ENCODING = 'utf-8'
FORMATO_MENSAJE_UNVUELO = '!32s32s32si'
FORMATO_FIN = '!c32s'
FORMATO_MENSAJE_UNAEROPUERTO = '!3sff'
FORMATO_MENSAJE_CABECERAAEROPUERTO = '!c32sH'
NOMBRE_COLA = 'cola_distancia'
NOMBRE_COLAAEROPUERTOS = 'cola_aeropuerto'


class AeropuertoNoEncontrado(Exception):
    "No se encontró el aeropuerto correspondiente a ese vuelo"
    pass

class ProtocoloFiltroDistancia(ProtocoloBase):
       
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
        id_vuelo, origen, destino, distancia = unpack(FORMATO_MENSAJE_UNVUELO, mensaje)        
        vuelo = Vuelo(id_vuelo.decode('utf-8'), origen.decode('utf-8').replace('\x00', ''), destino.decode('utf-8').replace('\x00', ''), 0, "", 0, distancia)
        return vuelo

    def callback_functionaero(self, body):
        # procesar los mensajes, llamando a procesar_vuelo o procesar_finvuelo segun corresponda
        logging.debug(f'llego mensaje AEROPUERTOS body: {body}')
        if body.startswith(IDENTIFICADOR_AEROPUERTO.encode('utf-8')):
            id_cliente, aeropuertos = self.traducir_aeropuertos(body)
            if self._recuperador.es_duplicado(id_cliente, body):
                logging.debug(f'Se recibió un aeropuerto duplicado: {body}')
                return
            contenido_persistir = self.procesar_aeropuerto(id_cliente, aeropuertos)
        elif body.startswith(IDENTIFICADOR_FIN_AEROPUERTO.encode('utf-8')):
            caracter, id_cliente = unpack(FORMATO_FIN, body)
            if self._recuperador.es_duplicado(id_cliente, body):
                logging.info(f'Se recibió un fin de aeropuertos duplicado: {body}')
                return
            contenido_persistir = self.procesar_finaeropuerto(id_cliente.decode('utf-8'))
        self._recuperador.almacenar(id_cliente, body, contenido_persistir)

    def callback_function(self, body):
        try:
            super().callback_function(body)
            return True
        except AeropuertoNoEncontrado:
            logging.error('Se intentó procesar procesar un vuelo de un aeropuerto que aún no había llegado, se reinterá más tarde')
            return False
    
    def iniciar(self, procesar_vuelo, procesar_finvuelo, procesar_aeropuerto, procesar_finaeropuerto,procesar_flush, id):
        self.corriendo = True
        self.procesar_vuelo = procesar_vuelo
        self.procesar_flush = procesar_flush
        self.procesar_finvuelo =  procesar_finvuelo
        self.procesar_aeropuerto =  procesar_aeropuerto
        self.procesar_finaeropuerto =  procesar_finaeropuerto
        
        self._colas.crear_cola_subscriptores(NOMBRE_COLAAEROPUERTOS)
        self._colas.subscribirse_cola(NOMBRE_COLAAEROPUERTOS, str(id), self.callback_functionaero)
        
        self._colas.crear_cola_por_topico(self.nombre_cola)
        self._colas.consumir_mensajes_por_topico(self.nombre_cola, str(id), self.callback_function, id, auto_ack=False, post_ack_callback=self.borrar_archivos)

        self._colas.consumir()

    def traducir_vuelo(self, vuelo):
        return  struct.pack(FORMATO_MENSAJE_UNVUELO,
                                      vuelo.id_vuelo.encode(STRING_ENCODING),
                                      vuelo.origen.encode(STRING_ENCODING),
                                      vuelo.destino.encode(STRING_ENCODING),
                                      int(vuelo.distancia))
        
        
    def traducir_aeropuertos(self, mensaje):        
        offset = calcsize(FORMATO_MENSAJE_CABECERAAEROPUERTO)
        tama_aero = calcsize(FORMATO_MENSAJE_UNAEROPUERTO)
        caracter, id_cliente, total_aeropuertos = unpack(FORMATO_MENSAJE_CABECERAAEROPUERTO, mensaje[0:offset])
        aeropuertos = []
        while offset < len(mensaje):
            aeropuerto_mensaje = mensaje[offset:offset + tama_aero]
            id_aeropuerto, latitud, longitud = unpack(FORMATO_MENSAJE_UNAEROPUERTO, aeropuerto_mensaje)
            aeropuerto = Aeropuerto(id_aeropuerto.decode('utf-8'), latitud, longitud)
            aeropuertos.append(aeropuerto)
            offset += tama_aero
        return id_cliente.decode('utf-8'), aeropuertos
        
    def enviar_aeropuertos(self, id_cliente, aeropuertos):
        mensaje_empaquetado = struct.pack(FORMATO_MENSAJE_CABECERAAEROPUERTO,
                                          IDENTIFICADOR_AEROPUERTO.encode(STRING_ENCODING),
                                          id_cliente.encode(STRING_ENCODING),
                                          len(aeropuertos))
                                      
        for aeropuerto in aeropuertos:
            mensaje_empaquetado += struct.pack(FORMATO_MENSAJE_UNAEROPUERTO,
                                          aeropuerto.id.encode(STRING_ENCODING),
                                          float(aeropuerto.latitud),
                                          float(aeropuerto.longitud)
                                          )
        self._colas.enviar_mensaje_suscriptores(NOMBRE_COLAAEROPUERTOS, mensaje_empaquetado)

    def enviar_fin_aeropuertos(self, id_cliente):
        mensaje = struct.pack(FORMATO_FIN, IDENTIFICADOR_FIN_AEROPUERTO.encode(STRING_ENCODING), id_cliente.encode(STRING_ENCODING)) 
        self._colas.enviar_mensaje_suscriptores(NOMBRE_COLAAEROPUERTOS, mensaje)


    def parar_vuelos(self):        
        self._colas.dejar_de_consumir(NOMBRE_COLA)

    def parar(self):        
        self._colas.dejar_de_consumir(NOMBRE_COLA)
        self._colas.dejar_de_consumir(NOMBRE_COLAAEROPUERTOS)

    def cerrar(self):
        self._colas.cerrar()
        super().cerrar()
        
        
        