import logging
import signal
from modelo.Aeropuerto import Aeropuerto
from modelo.Vuelo import Vuelo
from socket_comun import SocketComun, STATUS_ERR, STATUS_OK
from protocolo_cliente import ProtocoloCliente
from protocolo_resultados_cliente import ProtocoloResultadosCliente


class Client:
    def __init__(self, host, port):
        # Initialize server socket
        server_socket = SocketComun()
        server_socket.connect(host, port)
        #socket_enviar, socket_recibir = server_socket.split()
        self._protocolo = ProtocoloCliente(server_socket)
        self._protocolo_resultados = ProtocoloResultadosCliente(server_socket)
        signal.signal(signal.SIGTERM, self.sigterm_handler)

        
        

    def _enviar_vuelos(self, archivo_csv: str):
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
                    logging.error(f'accion: leer_vuelo | id vuelo: {id_vuelo}   origen: {origen}   destino: {destino}  precio: {precio} distancia: {distancia} duracion: {duracion} escalas: {escalas}')
                    self._protocolo.enviar_vuelo(vuelo)

        self._protocolo.enviar_fin_vuelos()
        
       
        

    def _enviar_aeropuertos(self, nombre_archivo: str):
        with open(nombre_archivo, 'r', encoding='utf-8') as archivo:
            next(archivo)  # Saltar la primera línea con encabezados
            for linea in archivo:
                campos = linea.strip().split(';')
                if len(campos) >= 7:
                    codigo = campos[0]  # Airport Code
                    latitud = float(campos[5])  # Latitude
                    longitud = float(campos[6])  # Longitude

                    aeropuerto = Aeropuerto(codigo, latitud, longitud)
                    self._protocolo.enviar_aeropuerto(aeropuerto)

        self._protocolo.enviar_fin_aeropuertos()


    def sigterm_handler(self, _signo, _stack_frame):
        logging.info('acción: sigterm_recibida')
        self._server_socket.close()
        logging.info(f'acción: cerrar_socket_servidor | resultado: OK')
        

    def run(self):
        

        logging.error('action: recibir_resultado | estado: esperando')
        estado, resultado = self._protocolo_resultados.recibir_resultado()
        if estado == STATUS_ERR:
            logging.error('action: recibir_resultado | resultado: error')
        else:
            logging.error(f'action: recibir_resultado | resultado: OK  | {resultado.convertir_a_str()}')

        self._enviar_aeropuertos('airports-codepublic.csv')
        self._enviar_vuelos('itineraries_short.csv')

        self._protocolo.cerrar()
        self._protocolo_resultados.cerrar()


                


            
    
        
        
        
           
