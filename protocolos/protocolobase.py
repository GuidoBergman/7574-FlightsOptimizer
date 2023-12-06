from abc import ABC, abstractmethod
import logging
from struct import unpack, pack, calcsize
from recuperador import Recuperador


STRING_ENCODING = 'utf-8'
FORMATOCABECERA_VUELO = '!c32sH'
IDENTIFICADOR_VUELO = 'V'
IDENTIFICADOR_FLUSH = 'L'
IDENTIFICADOR_FIN_VUELO = 'F'
FORMATO_FIN = '!c32sH'
FORMATO_FLUSH = '!c32s'
DEFAULT_ID = 1

class ProtocoloBase(ABC):

    def __init__(self):
        self._recuperador = Recuperador()
        self.vuelos_procesados = 0
        self.clientes_finalizados = []
    
    @abstractmethod
    def traducir_vuelo(self, vuelo):
        pass
    
    @abstractmethod
    def decodificar_vuelo(self, mensaje):
        pass
    
    def finalizo_cliente(self, id_cliente):
        logging.info(f"Cliente para finalizar: {id_cliente}")
        self.clientes_finalizados.append(id_cliente)
        
    def borrar_archivos(self):
        for arch in self.clientes_finalizados:
            logging.info(f"Eliminando archivo: {arch}")
            self._recuperador.eliminar_archivo(arch)
        self.clientes_finalizados = []
            
    def traducir_vuelos(self, id_cliente, vuelos):
        logging.debug(f"Traduccion para cliente {id_cliente} vuelos {len(vuelos)}")
        msg = pack(FORMATOCABECERA_VUELO, IDENTIFICADOR_VUELO.encode(STRING_ENCODING), id_cliente.encode(STRING_ENCODING), len(vuelos))
        for vuelo in vuelos:
            msg = msg + self.traducir_vuelo(vuelo)
        
        return msg
    
    def decodificar_vuelos(self, mensaje):
        vuelos = []
        offset = calcsize(FORMATOCABECERA_VUELO)
        caracter, id_cliente, total_vuelos = unpack(FORMATOCABECERA_VUELO, mensaje[0:offset])
        while offset < len(mensaje):
            vuelo_mensaje = mensaje[offset:offset + self.TAMANO_VUELO]
            vuelo = self.decodificar_vuelo(vuelo_mensaje)
            vuelos.append(vuelo)
            offset += self.TAMANO_VUELO
        return id_cliente.decode('utf-8'), vuelos
    
    def callback_function(self, body):
        # procesar los mensajes, llamando a procesar_vuelo o procesar_finvuelo segun corresponda
        contenido_a_persistir = None
        if body.startswith(IDENTIFICADOR_VUELO.encode('utf-8')):
            id_cliente, vuelos = self.decodificar_vuelos(body)
            if self._recuperador.es_duplicado(id_cliente, body):
                logging.debug(f'Se recibió un vuelo duplicado: {body}')
                return
            self.vuelos_procesados += len(vuelos)
            if (int(self.vuelos_procesados/len(vuelos)) % 300) == 1:
                logging.info(f'Procesando lote de vuelos: {self.vuelos_procesados}')
            contenido_a_persistir = self.procesar_vuelo(id_cliente, vuelos)
        elif body.startswith(IDENTIFICADOR_FIN_VUELO.encode('utf-8')):
            logging.debug(f'Body mensaje fin de vuelo recibido {body}')
            caracter, id_cliente, id_enviador = unpack(FORMATO_FIN, body)  
            id_cliente = id_cliente.decode('utf-8')
            if self._recuperador.es_duplicado(id_cliente, body):
                return
            logging.info(f"protocolo | RECIBE Fin de vuelo {id_cliente }")
            contenido_a_persistir = self.procesar_finvuelo(id_cliente)
        elif body.startswith(IDENTIFICADOR_FLUSH.encode('utf-8')):
            caracter, id_cliente = unpack(FORMATO_FLUSH, body)  
            id_cliente = id_cliente.decode('utf-8')
            if self._recuperador.es_duplicado(id_cliente, body):
                return
            logging.info(f"RECIBE FLUSH {id_cliente }")
            contenido_a_persistir = self.procesar_flush(id_cliente)

        self._recuperador.almacenar(id_cliente, body, contenido_a_persistir)
    

    def enviar_vuelos(self, id_cliente, vuelos):
        if (len(vuelos) == 0):
            return
        
        #Agrupa los vuelos por filtro
        grupos_de_vuelos = {}          
        for vuelo in vuelos:
            id_filtro_precio = (hash(vuelo.origen + vuelo.destino) % self._cant_filtros) + 1
            if id_filtro_precio not in grupos_de_vuelos:
                grupos_de_vuelos[id_filtro_precio] = []
            grupos_de_vuelos[id_filtro_precio].append(vuelo)
            
        #Envia a cada filtro su mensaje
        for id_filtro, vuelosfiltro in grupos_de_vuelos.items():
            mensaje_empaquetado = self.traducir_vuelos(id_cliente, vuelosfiltro)
            self._colas.enviar_mensaje_por_topico(self.nombre_cola, mensaje_empaquetado, id_filtro)
    
    def enviar_flush(self, id_cliente):
        logging.info(f"ENVIA FLUSH {id_cliente }")
        for i in range(1, self._cant_filtros + 1):
            mensaje = pack(FORMATO_FLUSH, IDENTIFICADOR_FLUSH.encode(STRING_ENCODING), id_cliente.encode(STRING_ENCODING))
            self._colas.enviar_mensaje_por_topico(self.nombre_cola,mensaje, i)

    def enviar_fin_vuelos(self, id_cliente):
        logging.info(f"ENVIA FIN VUELO {id_cliente }")
        for i in range(1, self._cant_filtros + 1):
            mensaje = pack(FORMATO_FIN, IDENTIFICADOR_FIN_VUELO.encode(STRING_ENCODING), id_cliente.encode(STRING_ENCODING), DEFAULT_ID)
            logging.debug(f'Body mensaje fin de vuelo enviado: {mensaje}')
            self._colas.enviar_mensaje_por_topico(self.nombre_cola,mensaje, i)

    def recuperar_siguiente_linea(self):
        for nombre_archivo, linea in self._recuperador.recuperar_siguiente_linea():
           yield nombre_archivo, linea

    def obtener_siguiente_linea_cliente(self, id_cliente):
        for linea in self._recuperador.obtener_siguiente_linea_cliente(id_cliente):
           yield linea

    def cerrar(self):
        self._recuperador.cerrar()