import logging
from struct import unpack, pack, calcsize
import struct
from modelo.Vuelo import Vuelo
from manejador_colas import ManejadorColas
from protocolobase import ProtocoloBase



TAMANIO_IDENTIFICADOR_MENSAJE = 1
IDENTIFICADOR_VUELO = 'V'
IDENTIFICADOR_AEROPUERTO = 'A'
IDENTIFICADOR_FIN_VUELO = 'F'
IDENTIFICADOR_FIN_AEROPUERTO = 'E'
IDENTIFICADOR_FLUSH = 'L'
FORMATO_FLUSH = '!c32s'
ESTADO_FIN_VUELOS = 1
ESTADO_FIN_AEROPUERTOS = 1

STRING_ENCODING = 'utf-8'
FORMATO_MENSAJE_UNVUELO = '!32s3s3sf'
FORMATO_MENSAJE_PROMEDIO = '!32sfiH'
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
        id, origen, destino, precio = unpack(FORMATO_MENSAJE_UNVUELO, mensaje)
        vuelo = Vuelo(id.decode('utf-8'), origen.decode('utf-8'), destino.decode('utf-8'), precio, "", 0, 0)
        return vuelo
    

    def callback_promedio_general(self, body):
        # procesar los mensajes, llamando a procesar_vuelo o procesar_finvuelo segun corresponda
        logging.debug(f'llego el promedio general: {body}')
        
        id_cliente, promedio_general = unpack("32sf", body)
        id_cliente = id_cliente.decode('utf-8')
        if self._recuperador.es_duplicado(id_cliente, body):
            logging.debug(f'Se recibió un promedio general duplicado: {body}')
            return
        contenido = self.procesar_promediogeneral(id_cliente, promedio_general)
        self._recuperador.almacenar(id_cliente, body, contenido)
        
    def callback_promedio(self, body):
        if body.startswith(IDENTIFICADOR_FLUSH.encode('utf-8')):
            caracter, id_cliente = unpack(FORMATO_FLUSH, body)  
            contenido = self.procesar_flush(id_cliente)
        else:   
            id_cliente, promedio, cantidad, id_filtro = unpack(FORMATO_MENSAJE_PROMEDIO, body)
            id_cliente = id_cliente.decode('utf-8')
            if self._recuperador.es_duplicado(id_cliente, body):
                    logging.debug(f'Se recibió un promedio duplicado: {body}')
                    return
            contenido = self.procesar_promedio(id_cliente, promedio, cantidad)
        self._recuperador.almacenar(id_cliente, body, contenido)

    def iniciar(self, procesar_vuelo, procesar_finvuelo, procesar_promediogeneral,procesar_flush, id):
        self.corriendo = True
        self.procesar_vuelo = procesar_vuelo
        self.procesar_finvuelo =  procesar_finvuelo
        self.procesar_flush = procesar_flush
        self.procesar_promediogeneral = procesar_promediogeneral
        self._colas.crear_cola_por_topico(self.nombre_cola)      
        self._colas.consumir_mensajes_por_topico(self.nombre_cola,  str(id), self.callback_function, id, post_ack_callback=self.borrar_archivos)
        

        self._colas.crear_cola_subscriptores(NOMBRE_COLAPROMEDIOGENERAL)
        self._colas.subscribirse_cola(NOMBRE_COLAPROMEDIOGENERAL, str(id), self.callback_promedio_general)            
        self._colas.consumir()


    def iniciar_promedio(self, procesar_promedio, procesar_flush):
        self.procesar_flush = procesar_flush
        self.procesar_promedio =  procesar_promedio        
        self._colas.crear_cola(NOMBRE_COLAPROMEDIO)
        self._colas.consumir_mensajes(NOMBRE_COLAPROMEDIO, self.callback_promedio, post_ack_callback=self.borrar_archivos)
        self._colas.consumir()
        
        
    
    def traducir_vuelo(self, vuelo):
        mensaje_empaquetado = struct.pack(FORMATO_MENSAJE_UNVUELO,
                                      vuelo.id_vuelo.encode(STRING_ENCODING),
                                      vuelo.origen.encode(STRING_ENCODING),
                                      vuelo.destino.encode(STRING_ENCODING),
                                      float(vuelo.precio)
                                      )           
        return mensaje_empaquetado
    
    def enviar_flush_promedio(self, id_cliente):
        logging.info(f"ENVIA FLUSH PROMEDIO {id_cliente }")
        mensaje = pack(FORMATO_FLUSH, IDENTIFICADOR_FLUSH.encode(STRING_ENCODING), id_cliente.encode(STRING_ENCODING))
        self._colas.enviar_mensaje(NOMBRE_COLAPROMEDIO, mensaje)

    def enviar_promedio(self, id_cliente, promedio: float, cantidad: int, id_filtro: int):
        logging.info(f"enviando promedio {promedio} cantidad {cantidad}")
        mensaje_empaquetado = struct.pack(FORMATO_MENSAJE_PROMEDIO,
                                      id_cliente.encode('utf-8'),
                                      promedio,
                                      cantidad,
                                      id_filtro)           
        
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
        
        
        