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
FORMATO_MENSAJE_VUELO = '!cH3s3sf'
FORMATO_MENSAJE_PROMEDIO = '!fi'
NOMBRE_COLA = 'cola_precios'
NOMBRE_COLAPROMEDIO = 'cola_promedios'
NOMBRE_COLAPROMEDIOGENERAL = 'cola_preciosgeneral'
HOST_COLAS = 'rabbitmq'


class ProtocoloFiltroPrecio:
       
    def __init__(self):    
       self.nombre_cola = NOMBRE_COLA
       self._colas = ManejadorColas(HOST_COLAS)
       self.corriendo = False
    

    def callback_function(self, body):
        # procesar los mensajes, llamando a procesar_vuelo o procesar_finvuelo segun corresponda
        logging.debug(f'llego mensaje body: {body}')
        if body.startswith(IDENTIFICADOR_VUELO.encode('utf-8')):
            self.procesar_vuelo(self.traducir_vuelo(body))
        else:
            self.procesar_finvuelo()

    def traducir_vuelo(self, mensaje):
        
        formato_mensaje = FORMATO_MENSAJE_VUELO
        tipomensaje, cantidad_vuelos, origen, destino, precio = unpack(formato_mensaje, mensaje)
        vuelo = Vuelo("", origen.decode('utf-8'), destino.decode('utf-8'), precio, "", 0, 0)
        return vuelo


    def callback_promedio_general(self, body):
        # procesar los mensajes, llamando a procesar_vuelo o procesar_finvuelo segun corresponda
        logging.info(f'llego el promedio general: {body}')
        pr = unpack("f", body)
        
        if type(pr) is tuple:
            pr = pr[0] 
        self.procesar_promediogeneral(pr)
        
    def callback_promedio(self, body):
        logging.info(f'llego un promedio: {body}') 
        un = unpack(FORMATO_MENSAJE_PROMEDIO, body)
        promedio, cantidad = un
        self.procesar_promedio(promedio, cantidad)

    def iniciar(self, procesar_vuelo, procesar_finvuelo, procesar_promediogeneral):
        self.corriendo = True
        self.procesar_vuelo = procesar_vuelo
        self.procesar_finvuelo =  procesar_finvuelo
        self.procesar_promediogeneral = procesar_promediogeneral
        
        self._colas.crear_cola(NOMBRE_COLAPROMEDIOGENERAL)
        self._colas.prepararconsumir_mensajes2(NOMBRE_COLAPROMEDIOGENERAL, self.callback_promedio_general)

        self._colas.crear_cola(self.nombre_cola)        
        self._colas.consumir_mensajes(self.nombre_cola, self.callback_function)


    def iniciar_promedio(self, procesar_promedio):
        self.procesar_promedio =  procesar_promedio        
        self._colas.crear_cola(NOMBRE_COLAPROMEDIO)
        self._colas.consumir_mensajes(NOMBRE_COLAPROMEDIO, self.callback_promedio)
        
    def enviar_vuelo(self, vuelo):
        tipo_mensaje = IDENTIFICADOR_VUELO.encode(STRING_ENCODING)
        tamanio_batch = 1        
        mensaje_empaquetado = struct.pack(FORMATO_MENSAJE_VUELO,
                                      tipo_mensaje,
                                      tamanio_batch,
                                      vuelo.origen.encode(STRING_ENCODING),
                                      vuelo.destino.encode(STRING_ENCODING),
                                      float(vuelo.precio)
                                      )           
        self._colas.enviar_mensaje(self.nombre_cola, mensaje_empaquetado)
        
    def enviar_fin_vuelos(self):
        self._colas.enviar_mensaje(self.nombre_cola, IDENTIFICADOR_FIN_VUELO.encode(STRING_ENCODING))
        
    def enviar_promedio(self, promedio: float, cantidad: int):
        logging.info(f"enviando promedio {promedio} cantidad {cantidad}")
        mensaje_empaquetado = struct.pack(FORMATO_MENSAJE_PROMEDIO,
                                      promedio,
                                      cantidad)           
        
        logging.info(f"enviando mensaje {mensaje_empaquetado}")
        self._colas.enviar_mensaje(NOMBRE_COLAPROMEDIO, mensaje_empaquetado)

    def enviar_promediogeneral(self, promedio: float):
        tipo_mensaje = IDENTIFICADOR_VUELO.encode(STRING_ENCODING)
        mensaje_empaquetado = struct.pack("f", promedio)           
        self._colas.enviar_mensaje(NOMBRE_COLAPROMEDIOGENERAL, mensaje_empaquetado)

    def parar_vuelos(self):        
        self.corriendo = False
        self._colas.dejar_de_consumir(NOMBRE_COLA)
        
        