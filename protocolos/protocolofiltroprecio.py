import logging
from struct import unpack, pack, calcsize
import struct
from modelo.Vuelo import Vuelo
from manejador_colas import ManejadorColas
from modelo.Aeropuerto import Aeropuerto
from modelo.estado import Estado
from protocolobase import ProtocoloBase
import uuid


TAMANIO_IDENTIFICADOR_MENSAJE = 1
IDENTIFICADOR_VUELO = 'V'
IDENTIFICADOR_AEROPUERTO = 'A'
IDENTIFICADOR_FIN_VUELO = 'F'
IDENTIFICADOR_FIN_AEROPUERTO = 'E'
IDENTIFICADOR_FLUSH = 'L'

ESTADO_FIN_VUELOS = 1
ESTADO_FIN_AEROPUERTOS = 1

STRING_ENCODING = 'utf-8'
FORMATO_MENSAJE_UNVUELO = '!3s3sf'
FORMATO_MENSAJE_PROMEDIO = '!32sfi'
NOMBRE_COLA = 'cola_precios'
NOMBRE_COLAPROMEDIO = 'cola_promedios'
NOMBRE_COLAPROMEDIOGENERAL = 'cola_preciosgeneral'
FORMATO_FIN_VUELO = '!c32s'



class ProtocoloFiltroPrecio(ProtocoloBase):
       
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
        origen, destino, precio = unpack(FORMATO_MENSAJE_UNVUELO, mensaje)
        vuelo = Vuelo("", origen.decode('utf-8'), destino.decode('utf-8'), precio, "", 0, 0)
        return vuelo
    

    def callback_promedio_general(self, body):
        # procesar los mensajes, llamando a procesar_vuelo o procesar_finvuelo segun corresponda
        logging.debug(f'llego el promedio general: {body}')
        id_cliente, promedio_general = unpack("32sf", body)
        id_cliente = id_cliente.decode('utf-8')
        contenido = self.procesar_promediogeneral(id_cliente, promedio_general)
        return id_cliente, contenido
        
    def callback_promedio(self, body):
        id_cliente, promedio, cantidad = unpack(FORMATO_MENSAJE_PROMEDIO, body)
        id_cliente = id_cliente.decode('utf-8')
        contenido = self.procesar_promedio(id_cliente, promedio, cantidad)
        return id_cliente, contenido

    def iniciar(self, procesar_vuelo, procesar_finvuelo, procesar_promediogeneral,procesar_flush, id):
        self.corriendo = True
        self.procesar_vuelo = procesar_vuelo
        self.procesar_finvuelo =  procesar_finvuelo
        self.procesar_flush = procesar_flush
        self.procesar_promediogeneral = procesar_promediogeneral
        self._colas.crear_cola_por_topico(self.nombre_cola)      
        self._colas.consumir_mensajes_por_topico(self.nombre_cola, self.callback_function, id)        
        

        self._colas.crear_cola_subscriptores(NOMBRE_COLAPROMEDIOGENERAL)
        self._colas.subscribirse_cola(NOMBRE_COLAPROMEDIOGENERAL, self.callback_promedio_general)            
        self._colas.consumir()


    def iniciar_promedio(self, procesar_promedio):
        self.procesar_promedio =  procesar_promedio        
        self._colas.crear_cola(NOMBRE_COLAPROMEDIO)
        self._colas.consumir_mensajes(NOMBRE_COLAPROMEDIO, self.callback_promedio)
        self._colas.consumir()
        
        
    
    def traducir_vuelo(self, vuelo):
        mensaje_empaquetado = struct.pack(FORMATO_MENSAJE_UNVUELO,
                                      vuelo.origen.encode(STRING_ENCODING),
                                      vuelo.destino.encode(STRING_ENCODING),
                                      float(vuelo.precio)
                                      )           
        return mensaje_empaquetado
        
    def enviar_promedio(self, id_cliente, promedio: float, cantidad: int):
        logging.debug(f"enviando promedio {promedio} cantidad {cantidad}")
        mensaje_empaquetado = struct.pack(FORMATO_MENSAJE_PROMEDIO,
                                      id_cliente.encode('utf-8'),
                                      promedio,
                                      cantidad)           
        
        logging.debug(f"enviando mensaje {mensaje_empaquetado}")
        self._colas.enviar_mensaje(NOMBRE_COLAPROMEDIO, mensaje_empaquetado)

    def enviar_promediogeneral(self, id_cliente, promedio: float):
        tipo_mensaje = IDENTIFICADOR_VUELO.encode(STRING_ENCODING)
        mensaje_empaquetado = struct.pack("32sf",id_cliente.encode('utf-8'), promedio)           
        self._colas.enviar_mensaje_suscriptores(NOMBRE_COLAPROMEDIOGENERAL, mensaje_empaquetado)

    def parar_vuelos(self):        
        self.corriendo = False
        self._colas.dejar_de_consumir(NOMBRE_COLA)

    def cerrar(self):
        self._colas.cerrar()
        super().cerrar()
        
        
        