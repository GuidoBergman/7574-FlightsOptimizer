from abc import ABC, abstractmethod
import logging
from struct import unpack, pack, calcsize


STRING_ENCODING = 'utf-8'
FORMATOCABECERA_VUELO = '!c32sH'
IDENTIFICADOR_VUELO = 'V'
IDENTIFICADOR_FLUSH = 'L'
IDENTIFICADOR_FIN_VUELO = 'F'
FORMATO_FIN = '!c32s'

class ProtocoloBase(ABC):
    
    @abstractmethod
    def traducir_vuelo(self, vuelo):
        pass
    
    @abstractmethod
    def decodificar_vuelo(self, mensaje):
        pass
    
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
        logging.debug(f'llego mensaje body: {body}')
        contenido_a_persisitir = None
        if body.startswith(IDENTIFICADOR_VUELO.encode('utf-8')):
            id_cliente, vuelos = self.decodificar_vuelos(body)
            contenido_a_persisitir = self.procesar_vuelo(id_cliente, vuelos)
        else:
            caracter, id_cliente = unpack(FORMATO_FIN, body)            
            if caracter == IDENTIFICADOR_FIN_VUELO:
                contenido_a_persisitir = self.procesar_finvuelo(id_cliente.decode('utf-8'))
            if caracter == IDENTIFICADOR_FLUSH:
                contenido_a_persisitir = self.procesar_flush(id_cliente.decode('utf-8'))

        return id_cliente, contenido_a_persisitir
    

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
        for i in range(1, self._cant_filtros + 1):
            mensaje = pack(FORMATO_FIN, IDENTIFICADOR_FLUSH.encode(STRING_ENCODING), id_cliente.encode(STRING_ENCODING))
            self._colas.enviar_mensaje_por_topico(self.nombre_cola,mensaje, i)

    def enviar_fin_vuelos(self, id_cliente):
        for i in range(1, self._cant_filtros + 1):
            mensaje = pack(FORMATO_FIN, IDENTIFICADOR_FIN_VUELO.encode(STRING_ENCODING), id_cliente.encode(STRING_ENCODING))
            self._colas.enviar_mensaje_por_topico(self.nombre_cola,mensaje, i)
