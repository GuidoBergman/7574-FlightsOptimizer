import logging
import signal
from modelo.Vuelo import Vuelo
from protocolo_cliente import ProtocoloCliente

TAMANIO_LOTE = 10000
    
class EnviadorVuelos:
    
    def __init__(self, protocoloCliente: ProtocoloCliente):
        self._protocolo = protocoloCliente

    def _sigterm_handler(self, _signo, _stack_frame):
        logging.info('action: sigterm_recibido (enviador vuelos)')
        self._protocolo.cerrar()
        

    # Hago un wrapper para el graceful exit
    def enviar_vuelos(self, archivo_csv: str):
        signal.signal(signal.SIGTERM, self._sigterm_handler)
        try:
           self._enviar_vuelos(archivo_csv)
        except (ConnectionResetError, BrokenPipeError, OSError):
            return

    def _enviar_vuelos(self, archivo_csv: str):
        
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
                        
                        logging.info(f"Envio lote de {TAMANIO_LOTE}")
                        self._protocolo.enviar_vuelos(lote)
                        lote = []
            if len(lote) > 0:
                self._protocolo.enviar_vuelos(lote)
        logging.error("Envio todos los vuelos")
        self._protocolo.enviar_fin_vuelos()

        self._protocolo.cerrar()