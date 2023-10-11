import logging
from struct import unpack, pack, calcsize
import struct
from manejador_colas import ManejadorColas
from modelo.Aeropuerto import Aeropuerto
from modelo.Vuelo import Vuelo
from modelo.estado import Estado

TAMANIO_IDENTIFICADOR_MENSAJE = 1
IDENTIFICADOR_VUELO = 'V'
IDENTIFICADOR_AEROPUERTO = 'A'
IDENTIFICADOR_FIN_VUELO = 'F'
IDENTIFICADOR_FIN_AEROPUERTO = 'E'

ESTADO_FIN_VUELOS = 1
ESTADO_FIN_AEROPUERTOS = 1

STRING_ENCODING = 'utf-8'
FORMATO_MENSAJE_VUELO = '!cH32s3s3s8s'


class ProtocoloFiltroVelocidad:
    


    def __init__(self, cant_filtros_velocidad=1):    
       self._colas = ManejadorColas('rabbitmq')
       self.corriendo = False
       self.nombre_cola = 'cola_FiltroVelocidad'
       
       self._cant_filtros_velocidad = int(cant_filtros_velocidad)
    
       

    def callback_function(self, body):
        # procesar los mensajes, llamando a procesar_vuelo o procesar_finvuelo segun corresponda
        logging.debug(f'llego mensaje body: {body}')
        if body.decode('utf-8').startswith(IDENTIFICADOR_VUELO):
            self.procesar_vuelo(self.traducir_vuelo(body))
        else:
            logging.debug(f'Se recibio un fin de vuelo, se nececitan {self._cant_filtros_escalas}')
            self._fines_vuelo += 1
            if self._fines_vuelo >= self._cant_filtros_escalas:
                self.procesar_finvuelo()

    def traducir_vuelo(self, mensaje):
        
        formato_mensaje = FORMATO_MENSAJE_VUELO
        tipomensaje, cantidad_vuelos, id_vuelo, origen, destino, duracion = unpack(formato_mensaje, mensaje)
        
        
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


    def enviar_vuelo(self, vuelo):
        tipo_mensaje = IDENTIFICADOR_VUELO.encode(STRING_ENCODING)
        tamanio_batch = 1
        
        mensaje_empaquetado = struct.pack(FORMATO_MENSAJE_VUELO,
                                      tipo_mensaje,
                                      tamanio_batch,
                                      vuelo.id_vuelo.encode(STRING_ENCODING),
                                      vuelo.origen.encode(STRING_ENCODING),
                                      vuelo.destino.encode(STRING_ENCODING),
                                      vuelo.duracion.encode(STRING_ENCODING)
                                      )
        
        id_filtro_velocidad = (hash(vuelo.origen + vuelo.destino) % self._cant_filtros_velocidad) + 1
        logging.debug(f"enviando vuelo con duracion {vuelo.duracion} encode: {vuelo.duracion.encode(STRING_ENCODING)} al filtro {id_filtro_velocidad}")


        
        self._colas.enviar_mensaje_por_topico(self.nombre_cola, mensaje_empaquetado, id_filtro_velocidad)



    def enviar_fin_vuelos(self):
        for i in range(1, self._cant_filtros_velocidad + 1):
            self._colas.enviar_mensaje_por_topico(self.nombre_cola,IDENTIFICADOR_FIN_VUELO.encode(STRING_ENCODING), i)



    def parar(self):        
        self.corriendo = False
        
        