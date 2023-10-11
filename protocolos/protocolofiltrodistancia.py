
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

IDENTIFICADOR_PROMEDIO = 'P'
IDENTIFICADOR_PROMEDIOGENERAL = 'G'

ESTADO_FIN_VUELOS = 1
ESTADO_FIN_AEROPUERTOS = 1

STRING_ENCODING = 'utf-8'
FORMATO_MENSAJE_VUELO = '!cH32s32s32si'
FORMATO_MENSAJE_AEROPUERTO = '!cH3sff'

NOMBRE_COLA = 'cola_distancia'
NOMBRE_COLAAEROPUERTOS = 'cola_aeropuerto'
HOST_COLAS = 'rabbitmq'


class ProtocoloFiltroDistancia:
       
    def __init__(self):    
       self.nombre_cola = NOMBRE_COLA
       self._colas = ManejadorColas(HOST_COLAS)
       self.corriendo = False

    def callback_function(self, body):
        # procesar los mensajes, llamando a procesar_vuelo o procesar_finvuelo segun corresponda
        logging.debug(f'llego mensajo VUELOS body: {body}')
        if body.startswith(IDENTIFICADOR_VUELO.encode('utf-8')):
            self.procesar_vuelo(self.traducir_vuelo(body))
        elif body.startswith(IDENTIFICADOR_FIN_VUELO.encode('utf-8')):
            self.procesar_finvuelo()
            
            

    def callback_functionaero(self, body):
        # procesar los mensajes, llamando a procesar_vuelo o procesar_finvuelo segun corresponda
        logging.debug(f'llego mensaje AEROPUERTOS body: {body}')
        if body.startswith(IDENTIFICADOR_AEROPUERTO.encode('utf-8')):
            self.procesar_aeropuerto(self.traducir_aeropuerto(body))
        elif body.startswith(IDENTIFICADOR_FIN_AEROPUERTO.encode('utf-8')):
            self.procesar_finaeropuerto()
            self._colas.dejar_de_consumir(NOMBRE_COLAAEROPUERTOS)
            self._colas.consumir_mensajes(self.nombre_cola, self.callback_function)

    def iniciar(self, procesar_vuelo, procesar_finvuelo, procesar_aeropuerto, procesar_finaeropuerto):
        self.corriendo = True
        self.procesar_vuelo = procesar_vuelo
        self.procesar_finvuelo =  procesar_finvuelo
        self.procesar_aeropuerto =  procesar_aeropuerto
        self.procesar_finaeropuerto =  procesar_finaeropuerto
        
        self._colas.crear_cola_subscriptores(NOMBRE_COLAAEROPUERTOS)        
        self._colas.crear_cola(self.nombre_cola)        
        self._colas.subscribirse_cola(NOMBRE_COLAAEROPUERTOS, self.callback_functionaero)
        self._colas.consumir()
        

        

        
        


    def traducir_vuelo(self, mensaje):        
        formato_mensaje = FORMATO_MENSAJE_VUELO
        tipomensaje, cantidad_vuelos, id_vuelo, origen, destino, distancia = unpack(formato_mensaje, mensaje)        
        vuelo = Vuelo(id_vuelo.decode('utf-8'), origen.decode('utf-8').replace('\x00', ''), destino.decode('utf-8').replace('\x00', ''), 0, "", 0, distancia)
        
        return vuelo

    def enviar_vuelo(self, vuelo):
        tipo_mensaje = IDENTIFICADOR_VUELO.encode(STRING_ENCODING)
        tamanio_batch = 1
        logging.debug(f'Enviando vuelo filtro distancia {vuelo.id_vuelo}')
        mensaje_empaquetado = struct.pack(FORMATO_MENSAJE_VUELO,
                                      tipo_mensaje,
                                      tamanio_batch,
                                      vuelo.id_vuelo.encode(STRING_ENCODING),
                                      vuelo.origen.encode(STRING_ENCODING),
                                      vuelo.destino.encode(STRING_ENCODING),
                                      int(vuelo.distancia)
                                      )
        self._colas.enviar_mensaje(self.nombre_cola, mensaje_empaquetado)

    def enviar_fin_vuelos(self):
        self._colas.enviar_mensaje(self.nombre_cola, IDENTIFICADOR_FIN_VUELO.encode(STRING_ENCODING))

    def traducir_aeropuerto(self, mensaje):        
        formato_mensaje = FORMATO_MENSAJE_AEROPUERTO
        tipomensaje, cantidad_aeropuertos, id_aeropuerto, latitud, longitud = unpack(formato_mensaje, mensaje)
        vuelo = Aeropuerto(id_aeropuerto.decode('utf-8'), latitud, longitud)
        return vuelo
        
    def enviar_aeropuerto(self, aeropuerto: Aeropuerto):
        tipo_mensaje = IDENTIFICADOR_AEROPUERTO.encode(STRING_ENCODING)
        tamanio_batch = 1
        logging.debug(f'Enviando aeropuerto filtro distancia {aeropuerto.id}')
        mensaje_empaquetado = struct.pack(FORMATO_MENSAJE_AEROPUERTO,
                                      tipo_mensaje,
                                      tamanio_batch,
                                      aeropuerto.id.encode(STRING_ENCODING),
                                      float(aeropuerto.latitud),
                                      float(aeropuerto.longitud)
                                      )
    
        
        self._colas.enviar_mensaje_suscriptores(NOMBRE_COLAAEROPUERTOS, mensaje_empaquetado)

    def enviar_fin_aeropuertos(self):
        self._colas.enviar_mensaje_suscriptores(NOMBRE_COLAAEROPUERTOS, IDENTIFICADOR_FIN_AEROPUERTO.encode(STRING_ENCODING))


    def parar_vuelos(self):        
        self._colas.dejar_de_consumir(NOMBRE_COLA)

    def parar(self):        
        self._colas.dejar_de_consumir(NOMBRE_COLA)
        self._colas.dejar_de_consumir(NOMBRE_COLAAEROPUERTOS)

    def cerrar(self):
        self._colas.cerrar()
        
        
        