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
FORMATO_MENSAJE_AEROPUERTO = '!H3sff'
FORMATO_MENSAJE_UNAEROPUERTO = '3sff'

FORMATO_MENSAJE_VUELO =  '!H32s3s3sfH50s8sh'
FORMATO_MENSAJE_UNVUELO = '!32s3s3sfH50s8sh'
FORMATO_TOTALES =  '!H'

from socket_comun import SocketComun, STATUS_ERR, STATUS_OK

class ProtocoloCliente:

    def __init__(self, socket):
        self._socket = socket


    def _recibir_identificador_mensaje(self):
        estado, mensaje = self._socket.receive(TAMANIO_IDENTIFICADOR_MENSAJE)
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
            estado, mensaje = self._socket.receive(tamanio_mensaje)
            
            
            if estado != STATUS_OK:
                logging.error(f'acción: recibir_vuelo | result: error')
                return STATUS_ERR, None
            cantidad_vuelos, id, origen, destino, precio, longitud_escalas, escalas, duracion, distancia = unpack(formato_mensaje, mensaje)
            id = id.decode(STRING_ENCODING)
            origen = origen.decode(STRING_ENCODING),
            destino = destino.decode(STRING_ENCODING)
            escalas = escalas[0:longitud_escalas].decode(STRING_ENCODING)
            duracion = duracion.decode(STRING_ENCODING)
            logging.debug(f"Vuelo con distancia: {distancia}")
            vuelo = Vuelo(id, origen, destino, precio, escalas, duracion, distancia)
            return STATUS_OK, vuelo
        elif identificador_mensaje == IDENTIFICADOR_FIN_VUELO:
            return ESTADO_FIN_VUELOS, None
        else:
            logging.error(f'acción: recibir_vuelo | result: error')
            return STATUS_ERR, None
        
    def recibir_vuelos(self):
        ret = []
        estado, identificador_mensaje = self._recibir_identificador_mensaje()
        if estado != STATUS_OK:
            logging.error(f'acción: recibir_vuelo | result: error')
            return STATUS_ERR, None        
        
        #si recibo identificador de vuelo
        if identificador_mensaje == IDENTIFICADOR_VUELO:
            formato_mensaje = FORMATO_MENSAJE_VUELO
            
            #Recibo la cantidad de vuelos
            tamanio_mensaje = calcsize(FORMATO_TOTALES)
            estado, tot_vuelos = self._socket.receive(tamanio_mensaje)
            if estado != STATUS_OK:
                logging.error(f'acción: recibir_vuelo | result: error')
                return STATUS_ERR, None
            
                
            cantidad_vuelos = unpack(FORMATO_TOTALES, tot_vuelos)
            logging.debug(f"recibo {cantidad_vuelos} vuelos")
            
            if type(cantidad_vuelos) is tuple:
                cantidad_vuelos = cantidad_vuelos[0] 

            tamanio_mensaje = calcsize(FORMATO_MENSAJE_UNVUELO)
            estado, mensaje = self._socket.receive(tamanio_mensaje * cantidad_vuelos)
            if estado != STATUS_OK:
                    logging.error(f'acción: recibir_vuelo | result: error')
                    return STATUS_ERR, None
            for i in range(cantidad_vuelos):
                
                primer_byte = i * tamanio_mensaje
                ultimo_byte = (i+1) * tamanio_mensaje
                parte_mensaje = mensaje[primer_byte: ultimo_byte]
                id_vuelo, origen, destino, precio, longitud_escalas, escalas, duracion, distancia = unpack(FORMATO_MENSAJE_UNVUELO, parte_mensaje)
                id_vuelo = id_vuelo.decode(STRING_ENCODING)
                origen = origen.decode(STRING_ENCODING),
                destino = destino.decode(STRING_ENCODING)
                escalas = escalas[0:longitud_escalas].decode(STRING_ENCODING)
                duracion = duracion.decode(STRING_ENCODING)
                
                logging.debug(f"Vuelo con distancia: {distancia}")
                vuelo = Vuelo(id_vuelo, origen, destino, precio, escalas, duracion, distancia)
                ret.append(vuelo)
                cantidad_vuelos -= 1

            return STATUS_OK, ret
        elif identificador_mensaje == IDENTIFICADOR_FIN_VUELO:
            return ESTADO_FIN_VUELOS, None
        else:
            logging.error(f'acción: recibir_vuelo | result: error')
            return STATUS_ERR, None
        

        
    def enviar_vuelos(self, vuelos):
        self._socket.send(IDENTIFICADOR_VUELO.encode(STRING_ENCODING), TAMANIO_IDENTIFICADOR_MENSAJE)
        
        tamanio_batch = len(vuelos)
        msg = pack(FORMATO_TOTALES, tamanio_batch)
        for vuelo in vuelos:
            msg += pack(FORMATO_MENSAJE_UNVUELO, 
                vuelo.id_vuelo.encode(STRING_ENCODING), vuelo.origen.encode(STRING_ENCODING), vuelo.destino.encode(STRING_ENCODING),
                vuelo.precio, len(vuelo.escalas), vuelo.escalas.encode(STRING_ENCODING), 
                vuelo.duracion.encode(STRING_ENCODING), vuelo.distancia)
        estado = self._socket.send(msg, len(msg))
        
        if estado == STATUS_ERR:
            logging.error("acción: enviar_vuelo | resultado: error")
            return STATUS_ERR

        return STATUS_OK

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
            logging.error("acción: enviar_vuelo | resultado: error")
            return STATUS_ERR

        return STATUS_OK



    def enviar_fin_vuelos(self):
        estado = self._socket.send(IDENTIFICADOR_FIN_VUELO.encode(STRING_ENCODING), TAMANIO_IDENTIFICADOR_MENSAJE)
        if estado == STATUS_ERR:
            logging.error("acción: enviar_fin_vuelos | resultado: error")
            return STATUS_ERR

        logging.debug(f'acción: enviar_fin_vuelos | resultado: OK')
        return STATUS_OK



    def recibir_aeropuertos(self):
        estado, identificador_mensaje = self._recibir_identificador_mensaje()
        if estado != STATUS_OK:
            logging.error(f'acción: recibir_aeropuerto | result: error')
            return STATUS_ERR, None

        if identificador_mensaje == IDENTIFICADOR_AEROPUERTO:            
            tamanio_mensaje = calcsize(FORMATO_TOTALES)
            estado, total_aeropuertos_b = self._socket.receive(tamanio_mensaje)
            if estado != STATUS_OK:
                logging.error(f'acción: recibir_aeropuerto | result: error')
                return STATUS_ERR, None
            total_aeropuertos = unpack(FORMATO_TOTALES, total_aeropuertos_b)
            
            if type(total_aeropuertos) is tuple:
                total_aeropuertos = total_aeropuertos[0] 
            aeropuertos = []            
            
            for i in range(total_aeropuertos):
                # Recibir el mensaje de cada aeropuerto y verificar el estado
                tamanio_mensaje = calcsize(FORMATO_MENSAJE_UNAEROPUERTO)
                estado, mensaje = self._socket.receive(tamanio_mensaje)
                if estado != STATUS_OK:
                    logging.error(f'acción: recibir_aeropuerto | result: error')
                    return STATUS_ERR, None
                # Desempaquetar el mensaje y obtener el id, la latitud y la longitud de cada aeropuerto
                id_aeropuerto, latitud, longitud = unpack(FORMATO_MENSAJE_UNAEROPUERTO, mensaje)
                logging.debug(f'Desempaca vuelo {id_aeropuerto} - {latitud} - {longitud}')
                # Crear un objeto aeropuerto con esos datos y agregarlo al vector
                aeropuerto = Aeropuerto(id_aeropuerto.decode(STRING_ENCODING), latitud, longitud)
                aeropuertos.append(aeropuerto)
            return STATUS_OK, aeropuertos
        elif identificador_mensaje == IDENTIFICADOR_FIN_AEROPUERTO:
            return ESTADO_FIN_AEROPUERTOS, None

    def enviar_aeropuertos(self, aeropuertos):
        logging.info("enviando aeropuertos ...")
        self._socket.send(IDENTIFICADOR_AEROPUERTO.encode(STRING_ENCODING), TAMANIO_IDENTIFICADOR_MENSAJE)
        tamanio_batch = len(aeropuertos)
        msg = pack(FORMATO_TOTALES, tamanio_batch)
        for aeropuerto in aeropuertos:
            msg += pack(FORMATO_MENSAJE_UNAEROPUERTO, aeropuerto.id.encode(STRING_ENCODING), aeropuerto.latitud, aeropuerto.longitud)
        estado = self._socket.send(msg, len(msg))
        
        if estado == STATUS_ERR:
            logging.error("acción: enviar_vuelo | resultado: error")
            return STATUS_ERR

        return STATUS_OK

    def enviar_fin_aeropuertos(self):
        estado = self._socket.send(IDENTIFICADOR_FIN_AEROPUERTO.encode(STRING_ENCODING), TAMANIO_IDENTIFICADOR_MENSAJE)
        if estado == STATUS_ERR:
            logging.error("acción: enviar_fin_aeorpuertos | resultado: error")
            return STATUS_ERR

        logging.debug(f'acción: enviar_fin_aeorpuertos | resultado: OK')
        return STATUS_OK

    def cerrar(self):
        self._socket.close()