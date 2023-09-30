import logging
import signal
from filtros.modelo.Vuelo import Vuelo
from socket_comun import SocketComun, STATUS_ERR, STATUS_OK



class Client:
    def __init__(self, host, port):
        # Initialize server socket
        self._server_socket = SocketComun()
        self._server_socket.connect(host, port)
        signal.signal(signal.SIGTERM, self.sigterm_handler)

        
        

    def cargar_vuelos_desde_csv(archivo_csv: str) -> List[Vuelo]:
        vuelos = []
        with open(archivo_csv, 'r', encoding='utf-8') as file:
            next(file)  # Saltar la primera línea con encabezados
            for line in file:
                fields = line.strip().split(',')
                if len(fields) >= 4:
                    id_vuelo = fields[0]  # legId
                    origen = fields[3]  # startingAirport
                    destino = fields[4]  # destinationAirport
                    trayecto = f"{origen}-{destino}"
                    precio = float(fields[11])  # totalFare
                    segmentos = fields[22].split("||")  # segmentsAirlineName
                    vuelo = Vuelo(id_vuelo, trayecto, precio, segmentos)
                    vuelos.append(vuelo)
        return vuelos
        

    def sigterm_handler(self, _signo, _stack_frame):
        logging.info('action: sigterm_received')
        self._server_socket.close()
        logging.info(f'action: close_server_socket | result: success')
        

    def run(self):
            self._server_socket.send('hola'.encode('utf-8'), 4)

                


            
    
        
        
        
           
