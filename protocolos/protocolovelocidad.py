import logging
from struct import unpack, pack, calcsize
import struct
from manejador_colas import ManejadorColas
from modelo.Aeropuerto import Aeropuerto
from modelo.Vuelo import Vuelo
from modelo.estado import Estado
from protocolobase import ProtocoloBase

TAMANIO_IDENTIFICADOR_MENSAJE = 1
IDENTIFICADOR_VUELO = 'V'
IDENTIFICADOR_AEROPUERTO = 'A'
IDENTIFICADOR_FIN_VUELO = 'F'
IDENTIFICADOR_FIN_AEROPUERTO = 'E'

ESTADO_FIN_VUELOS = 1
ESTADO_FIN_AEROPUERTOS = 1

STRING_ENCODING = 'utf-8'
FORMATO_MENSAJE_UNVUELO = '!32s3s3s8s'
FORMATO_FIN_VUELO = '!c32s'


class ProtocoloFiltroVelocidad(ProtocoloBase):

    def __init__(self, cant_filtros_velocidad=1):    
       self.TAMANO_VUELO = calcsize(FORMATO_MENSAJE_UNVUELO)
       self._colas = ManejadorColas()
       self.corriendo = False
       self.nombre_cola = 'cola_FiltroVelocidad'       
       self._cant_filtros_velocidad = int(cant_filtros_velocidad)
    
       

    def callback_function(self, body):
        # procesar los mensajes, llamando a procesar_vuelo o procesar_finvuelo segun corresponda
        if body.decode('utf-8').startswith(IDENTIFICADOR_VUELO):
            id_cliente, vuelos = self.decodificar_vuelos(body)
            self.procesar_vuelo(id_cliente, vuelos)
        else:            
            caracter, id_cliente = unpack(FORMATO_FIN_VUELO, body)            
            self._fines_vuelo += 1
            if self._fines_vuelo >= self._cant_filtros_escalas:
                self.procesar_finvuelo(id_cliente.decode('utf-8'))

    def decodificar_vuelo(self, mensaje):        
        id_vuelo, origen, destino, duracion = unpack(FORMATO_MENSAJE_UNVUELO, mensaje)
        vuelo = Vuelo(id_vuelo.decode('utf-8'), origen.decode('utf-8'), destino.decode('utf-8'), 0, "", duracion.decode('utf-8'), 0)
        return vuelo


    def iniciar(self, procesar_vuelo, procesar_finvuelo, id, cant_filtros_escalas):
        self.corriendo = True
        self.procesar_vuelo = procesar_vuelo
        self.procesar_finvuelo =  procesar_finvuelo
        self._cant_filtros_escalas = cant_filtros_escalas
        self._fines_vuelo = 0
        self._colas.crear_cola_por_topico(self.nombre_cola)
        self._colas.consumir_mensajes_por_topico(self.nombre_cola, self.callback_function, id)
        self._colas.consumir()

    def enviar_vuelos(self, id_cliente, vuelos):
        if (len(vuelos) == 0):
            return
        logging.info(f'Envio los vuelos a velocidad del cliente {id_cliente}')
        grupos_de_vuelos = {}          
        for vuelo in vuelos:
            id_filtro_velocidad = (hash(vuelo.origen + vuelo.destino) % self._cant_filtros_velocidad) + 1
            if id_filtro_velocidad not in grupos_de_vuelos:
                grupos_de_vuelos[id_filtro_velocidad] = []
            grupos_de_vuelos[id_filtro_velocidad].append(vuelo)
        for id_filtro, vuelosfiltro in grupos_de_vuelos.items():
            self.enviar_vuelos_filtro(id_cliente, id_filtro, vuelosfiltro)
     
    def enviar_vuelos_filtro(self, id_cliente, id_filtro, vuelos):
        mensaje_empaquetado = self.traducir_vuelos(id_cliente, vuelos)
        self._colas.enviar_mensaje_por_topico(self.nombre_cola, mensaje_empaquetado, id_filtro)
    
    def traducir_vuelo(self, vuelo):
        return struct.pack(FORMATO_MENSAJE_UNVUELO,
                                      vuelo.id_vuelo.encode(STRING_ENCODING),
                                      vuelo.origen.encode(STRING_ENCODING),
                                      vuelo.destino.encode(STRING_ENCODING),
                                      vuelo.duracion.encode(STRING_ENCODING))

    def enviar_fin_vuelos(self, id_cliente):
        for i in range(1, self._cant_filtros_velocidad + 1):
            mensaje = pack(FORMATO_FIN_VUELO, IDENTIFICADOR_FIN_VUELO.encode(STRING_ENCODING), id_cliente.encode(STRING_ENCODING))
            self._colas.enviar_mensaje_por_topico(self.nombre_cola,mensaje, i)



    def parar(self):        
        self.corriendo = False
        
    def cerrar(self):
        self._colas.cerrar()
        