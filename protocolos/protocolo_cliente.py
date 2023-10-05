from modelo.Vuelo import Vuelo
from modelo.Aeropuerto import Aeropuerto
from struct import unpack, pack, calcsize
import logging

TAMANIO_IDENTIFICADOR_MENSAJE = 1
IDENTIFICADOR_VUELO = 'V'
IDENTIFICADOR_AEROPUERTO = 'A'
IDENTIFICADOR_FIN_VUELO = 'F'
IDENTIFICADOR_FIN_AEROPUERTO = 'E'

ESTADO_FIN_VUELOS = 1
ESTADO_FIN_AEROPUERTOS = 1

STRING_ENCODING = 'utf-8'
FORMATO_MENSAJE_VUELO = '!H32s3s3sfH50s8sh'
FORMATO_MENSAJE_AEROPUERTO = '!H3sff'

from socket_comun import SocketComun, STATUS_ERR, STATUS_OK

class ProtocoloCliente:

    def __init__(self, socket):
        self._socket = socket


    def _recibir_identificador_mensaje(self):
        estado, mensaje, _ = self._socket.receive(TAMANIO_IDENTIFICADOR_MENSAJE)
        if estado != STATUS_OK:
            return STATUS_ERR, None
        return estado, mensaje.decode(STRING_ENCODING)

    def recibir_vuelo(self):
        estado, identificador_mensaje = self._recibir_identificador_mensaje()
        if estado != STATUS_OK:
            logging.error(f'acción: recibir_vuelo | result: error')
            return STATUS_ERR, None

        if identificador_mensaje == IDENTIFICADOR_VUELO:
            formato_mensaje = FORMATO_MENSAJE_VUELO
            tamanio_mensaje = calcsize(formato_mensaje)
            estado, mensaje, _ = self._socket.receive(tamanio_mensaje)
            
            if estado != STATUS_OK:
                logging.error(f'acción: recibir_vuelo | result: error')
                return STATUS_ERR, None
            cantidad_vuelos, id, origen, destino, precio, longitud_escalas, escalas, duracion, distancia = unpack(formato_mensaje, mensaje)
            id = id.decode(STRING_ENCODING)
            origen = origen.decode(STRING_ENCODING),
            destino = destino.decode(STRING_ENCODING)
            escalas = escalas[0:longitud_escalas].decode(STRING_ENCODING)
            duracion = duracion.decode(STRING_ENCODING)

            vuelo = Vuelo(id, origen, destino, precio, escalas, duracion, distancia)
            return STATUS_OK, vuelo
        elif identificador_mensaje == IDENTIFICADOR_FIN_VUELO:
            return ESTADO_FIN_VUELOS, None
        


    def enviar_vuelo(self, vuelo):
        self._socket.send(IDENTIFICADOR_VUELO.encode(STRING_ENCODING), TAMANIO_IDENTIFICADOR_MENSAJE)
        tamanio_batch = 1
        formato_mensaje = FORMATO_MENSAJE_VUELO
        tamanio_mensaje = calcsize(formato_mensaje)
        msg = pack(formato_mensaje, tamanio_batch,
            vuelo.id_vuelo.encode(STRING_ENCODING), vuelo.origen.encode(STRING_ENCODING), vuelo.destino.encode(STRING_ENCODING),
            vuelo.precio, len(vuelo.escalas), vuelo.escalas.encode(STRING_ENCODING), 
            vuelo.duracion.encode(STRING_ENCODING), vuelo.distancia
        )
        estado = self._socket.send(msg, tamanio_mensaje)
        if estado == STATUS_ERR:
            logging.error("acción: enviar_mensaje | resultado: error")
            return STATUS_ERR



    def enviar_fin_vuelos(self):
        self._socket.send(IDENTIFICADOR_FIN_VUELO.encode(STRING_ENCODING), TAMANIO_IDENTIFICADOR_MENSAJE)



    def recibir_aeropuerto(self):
        estado, identificador_mensaje = self._recibir_identificador_mensaje()
        if estado != STATUS_OK:
            logging.error(f'acción: recibir_aeropuerto | result: error')
            return STATUS_ERR, None

        if identificador_mensaje == IDENTIFICADOR_AEROPUERTO:
            formato_mensaje = FORMATO_MENSAJE_AEROPUERTO
            tamanio_mensaje = calcsize(formato_mensaje)
            estado, mensaje, _ = self._socket.receive(tamanio_mensaje)
            if estado != STATUS_OK:
                logging.error(f'acción: recibir_aeropuerto | result: error')
                return STATUS_ERR, None
            cantidad_aeropuertos, id, latitud, longitud = unpack(formato_mensaje, mensaje)
            id = id.decode(STRING_ENCODING)
            aeropuerto = Aeropuerto(id, latitud, longitud)
            
            return STATUS_OK, aeropuerto
        elif identificador_mensaje == IDENTIFICADOR_FIN_AEROPUERTO:
            return ESTADO_FIN_AEROPUERTOS, None

    def enviar_aeropuerto(self, aeropuerto):
        self._socket.send(IDENTIFICADOR_AEROPUERTO.encode(STRING_ENCODING), TAMANIO_IDENTIFICADOR_MENSAJE)
        tamanio_batch = 1
        formato_mensaje = FORMATO_MENSAJE_AEROPUERTO
        tamanio_mensaje = calcsize(formato_mensaje)
        msg = pack(formato_mensaje, tamanio_batch,
            aeropuerto.id.encode(STRING_ENCODING), aeropuerto.latitud, aeropuerto.longitud
        )
        estado = self._socket.send(msg, tamanio_mensaje)
        if estado == STATUS_ERR:
            logging.error("acción: enviar_mensaje | resultado: error")
            return STATUS_ERR



    def enviar_fin_aeropuertos(self):
        self._socket.send(IDENTIFICADOR_FIN_AEROPUERTO.encode(STRING_ENCODING), TAMANIO_IDENTIFICADOR_MENSAJE)