import logging
import signal
from modelo.Aeropuerto import Aeropuerto
from modelo.Vuelo import Vuelo
from socket_comun import SocketComun, STATUS_ERR, STATUS_OK
from protocolo_cliente import ProtocoloCliente


class Client:
    def __init__(self, host, port):
        # Initialize server socket
        self._server_socket = SocketComun()
        self._server_socket.connect(host, port)
        self._protocolo = ProtocoloCliente(self._server_socket)
        signal.signal(signal.SIGTERM, self.sigterm_handler)

        
        

    def _enviar_vuelos(self, archivo_csv: str) -> [Vuelo]:
        vuelos = []
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
                    logging.info(f'id vuelo: {id_vuelo}   origen: {origen}   destino: {destino}  precio: {precio} distancia: {distancia} duracion: {duracion} escalas: {escalas}')
                    #vuelos.append(vuelo)
                    self._protocolo.enviar_vuelo(vuelo)

        self._protocolo.enviar_fin_vuelos()
        return vuelos
        

    def _enviar_aeropuertos(self, nombre_archivo: str) -> [Aeropuerto]:
        aeropuertos = []

        with open(nombre_archivo, 'r', encoding='utf-8') as archivo:
            next(archivo)  # Saltar la primera línea con encabezados
            for linea in archivo:
                campos = linea.strip().split(';')
                if len(campos) >= 7:
                    codigo = campos[0]  # Airport Code
                    latitud = float(campos[5])  # Latitude
                    longitud = float(campos[6])  # Longitude

                    aeropuerto = Aeropuerto(codigo, latitud, longitud)
                    aeropuertos.append(aeropuerto)

        return aeropuertos


    def sigterm_handler(self, _signo, _stack_frame):
        logging.info('action: sigterm_received')
        self._server_socket.close()
        logging.info(f'action: close_server_socket | result: success')
        

    def run(self):
            self._enviar_vuelos('itineraries_short.csv')

                


            
    
        
        
        
           
