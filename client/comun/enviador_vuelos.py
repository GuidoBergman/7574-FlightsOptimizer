import logging
from re import L
import signal
import os

from modelo.Vuelo import Vuelo
from protocolo_cliente import ProtocoloCliente

TAMANIO_LOTE = 40
    
class EnviadorVuelos:
    
    def __init__(self, protocoloCliente: ProtocoloCliente):
        self._protocolo = protocoloCliente

    def _sigterm_handler(self, _signo, _stack_frame):
        logging.info('action: sigterm_recibida (enviador vuelos)')
        self._protocolo.cerrar()
        

    # Hago un wrapper para el graceful exit
    def enviar_vuelos(self, archivo_csv: str):
        signal.signal(signal.SIGTERM, self._sigterm_handler)
        try:
           self._enviar_vuelos(archivo_csv)
        except (ConnectionResetError, BrokenPipeError, OSError):
            return

    def _enviar_vuelos(self, archivo_csv: str):
        
        # Obtiene el tamaño del archivo en bytes
        tamanio_bytes = os.path.getsize(archivo_csv)
        lotes_estimados = int((tamanio_bytes / 372.24) / TAMANIO_LOTE)
        logging.info(f"Lotes estimados: {lotes_estimados}")
        lotes_enviados = 0;
        
        lote = []        
        with open(archivo_csv, 'r', encoding='utf-8') as file:
            next(file)  # Saltar la primera línea con encabezados
            for line in file:
                fields = line.strip().split(',')
                if len(fields) >= 4:
                    id_vuelo = fields[0]  # legId
                    origen = fields[3]  # startingAirport
                    destino = fields[4]  # destinationAirport
                    duracion = fields[6] #travelDuration
                    precio = float(fields[12])  # totalFare
                    try:
                        distancia = int(fields[14])  # totalTravelDistance
                    except ValueError:
                        distancia = -1
                    escalas = fields[19]  # segmentsArrivalAirportCode
                    vuelo = Vuelo(id_vuelo, origen, destino, precio, escalas, duracion, distancia)
                    logging.debug(f'accion: leer_vuelo | id vuelo: {id_vuelo}   origen: {origen}   destino: {destino}  precio: {precio} distancia: {distancia} duracion: {duracion} escalas: {escalas}')
                    lote.append(vuelo)
                    if (len(lote) >= TAMANIO_LOTE):
                        
                        logging.debug(f"Envio lote {lotes_enviados} de {TAMANIO_LOTE}")
                        self._protocolo.enviar_vuelos(lote)
                        lote = []
                        lotes_enviados += 1
                        if (lotes_enviados % 300) == 1:
                            logging.info(f"Lotes enviados: {lotes_enviados} sobre {lotes_estimados} (Estimados)")
            if len(lote) > 0:
                self._protocolo.enviar_vuelos(lote)
        logging.info(f"Envio todos los vuelos. Lotes enviados: {lotes_enviados}")
        self._protocolo.enviar_fin_vuelos()

        self._protocolo.cerrar()