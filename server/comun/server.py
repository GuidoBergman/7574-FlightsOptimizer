import logging
import signal
from protocolo_cliente import ProtocoloCliente, ESTADO_FIN_VUELOS, ESTADO_FIN_AEROPUERTOS
from comun.handler import Handler
from multiprocessing import Process, Manager

from socket_comun import SocketComun

CANT_HANDLERS = 3
EOF_MSG = 'EOF'

class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = SocketComun()
        self._server_socket.bind_and_listen('', port, listen_backlog)
        signal.signal(signal.SIGTERM, self.sigterm_handler)
        

        
        

        

    def sigterm_handler(self, _signo, _stack_frame):
        logging.info('action: sigterm_received')
        self._server_socket.close()
        logging.info(f'action: close_server_socket | result: success')
        

    def _recibir_vuelos(self, protocolo_cliente, vuelos):
        while True:
            
            estado, vuelo = protocolo_cliente.recibir_vuelo()
            if estado == ESTADO_FIN_VUELOS:
                #vuelos.put(EOF_MSG)
                break
            
            vuelos.put(vuelo)
            logging.info(f'Vuelo recibido (server):  id vuelo: {vuelo.id_vuelo}   origen: {vuelo.origen}   destino: {vuelo.destino}  precio: {vuelo.precio} distancia: {vuelo.distancia} duracion: {vuelo.duracion} escalas: {vuelo.escalas}')



    def _recibir_aeropuertos(self, protocolo_cliente):
        while True:            
            estado, aeropuerto = protocolo_cliente.recibir_aeropuerto()
            if estado == ESTADO_FIN_AEROPUERTOS:
                break

            logging.info(f'Aeropuerto recibido:  id: {aeropuerto.id}   latitud: {aeropuerto.latitud}   longitud: {aeropuerto.longitud}')



    def run(self):
            
            try:
                client_sock, addr = self._server_socket.accept()
            except OSError:
                return

            procesos = []
            with Manager() as manager:
                vuelos = manager.Queue()
                for i in range(CANT_HANDLERS):
                    handler = Handler()
                    handler_process = Process(target=handler.run, args=((vuelos),))
                    handler_process.start()
                    procesos.append(handler_process)


                protocolo_cliente = ProtocoloCliente(client_sock)  
                self._recibir_aeropuertos(protocolo_cliente)  
                self._recibir_vuelos(protocolo_cliente, vuelos)


                client_sock.close()

                for proceso in procesos:
                    proceso.terminate()
                    proceso.join()

            
    
        
        
        
           
