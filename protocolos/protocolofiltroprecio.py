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
       
    def __init__(self, cant_filtros_precio=None, id_cliente=None):
       self.TAMANO_VUELO = calcsize(FORMATO_MENSAJE_UNVUELO)
       self.nombre_cola = NOMBRE_COLA
       self._colas = ManejadorColas()
       self.corriendo = False       
       if cant_filtros_precio:
          self._cant_filtros_precio= int(cant_filtros_precio)        
       self.id_cliente = id_cliente
       

    def decodificar_vuelo(self, mensaje):        
        origen, destino, precio = unpack(FORMATO_MENSAJE_UNVUELO, mensaje)
        vuelo = Vuelo("", origen.decode('utf-8'), destino.decode('utf-8'), precio, "", 0, 0)
        return vuelo
    

    def callback_promedio_general(self, body):
        # procesar los mensajes, llamando a procesar_vuelo o procesar_finvuelo segun corresponda
        logging.debug(f'llego el promedio general: {body}')
        id_cliente, promedio_general = unpack("32sf", body)
        self.procesar_promediogeneral(id_cliente.decode('utf-8'), promedio_general)
        
    def callback_promedio(self, body):
        id_cliente, promedio, cantidad = unpack(FORMATO_MENSAJE_PROMEDIO, body)
        self.procesar_promedio(id_cliente.decode('utf-8'), promedio, cantidad)

    def iniciar(self, procesar_vuelo, procesar_finvuelo, procesar_promediogeneral, id):
        self.corriendo = True
        self.procesar_vuelo = procesar_vuelo
        self.procesar_finvuelo =  procesar_finvuelo
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
        
    def enviar_vuelos(self, vuelos):
        if (len(vuelos) == 0):
            return
        #Agrupa los vuelos por filtro
        grupos_de_vuelos = {}          
        for vuelo in vuelos:
            id_filtro_precio = (hash(vuelo.origen + vuelo.destino) % self._cant_filtros_precio) + 1
            if id_filtro_precio not in grupos_de_vuelos:
                grupos_de_vuelos[id_filtro_precio] = []
            grupos_de_vuelos[id_filtro_precio].append(vuelo)
            
        #Envia a cada filtro su mensaje
        for id_filtro, vuelosfiltro in grupos_de_vuelos.items():
            self.enviar_vuelos_filtro(self.id_cliente, id_filtro, vuelosfiltro)
    
    def enviar_vuelos_filtro(self, id_cliente, id_filtro, vuelos):
        logging.debug(f'Enviando vuelos: {len(vuelos)} al filtro {id_filtro}')
        mensaje_empaquetado = self.traducir_vuelos(id_cliente, vuelos)
        self._colas.enviar_mensaje_por_topico(self.nombre_cola, mensaje_empaquetado, id_filtro)
    
    def traducir_vuelo(self, vuelo):
        mensaje_empaquetado = struct.pack(FORMATO_MENSAJE_UNVUELO,
                                      vuelo.origen.encode(STRING_ENCODING),
                                      vuelo.destino.encode(STRING_ENCODING),
                                      float(vuelo.precio)
                                      )           
        return mensaje_empaquetado
       

    def enviar_fin_vuelos(self, id_cliente):
        mensaje = pack(FORMATO_FIN_VUELO, IDENTIFICADOR_FIN_VUELO.encode(STRING_ENCODING), id_cliente.encode(STRING_ENCODING))
        for i in range(1, self._cant_filtros_precio + 1):
            logging.info(f"Envio fin de vuelto al filtro {i}")
            self._colas.enviar_mensaje_por_topico(self.nombre_cola, mensaje, i)
        
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
        
        
        