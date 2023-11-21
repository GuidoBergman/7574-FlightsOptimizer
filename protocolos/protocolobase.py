from abc import ABC, abstractmethod
import logging
from struct import unpack, pack, calcsize


STRING_ENCODING = 'utf-8'
FORMATOCABECERA_VUELO = '!c32sH'
IDENTIFICADOR_VUELO = 'V'
FORMATO_FIN_VUELO = '!c32s'

class ProtocoloBase(ABC):
    
    @abstractmethod
    def traducir_vuelo(self, vuelo):
        pass
    
    @abstractmethod
    def decodificar_vuelo(self, mensaje):
        pass
    
    def traducir_vuelos(self, id_cliente, vuelos):
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
        if body.decode('utf-8').startswith(IDENTIFICADOR_VUELO):
            id_cliente, vuelos = self.decodificar_vuelos(body)
            logging.info(f"recibi del cliente {id_cliente} vuelos: {vuelos}")
            self.procesar_vuelo(id_cliente, vuelos)
        else:
            caracter, id_vuelo = unpack(FORMATO_FIN_VUELO, body)            
            self.procesar_finvuelo(id_vuelo.decode('utf-8'))