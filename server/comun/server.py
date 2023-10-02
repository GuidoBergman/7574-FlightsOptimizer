import logging
import signal
from socket_comun import SocketComun, STATUS_ERR, STATUS_OK
from manejador_colas import ManejadorColas
from protocolo_cliente import ProtocoloCliente, ESTADO_FIN_VUELOS, ESTADO_FIN_AEROPUERTOS



class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = SocketComun()
        self._server_socket.bind_and_listen('', port, listen_backlog)
        signal.signal(signal.SIGTERM, self.sigterm_handler)
        self._colas = ManejadorColas('rabbitmq')

        
        

        

    def sigterm_handler(self, _signo, _stack_frame):
        logging.info('action: sigterm_received')
        self._server_socket.close()
        logging.info(f'action: close_server_socket | result: success')
        

    def _recibir_vuelos(self, protocolo_cliente):
        while True:
            self._colas.enviar_mensaje('cola', 'botella')
            
            estado, vuelo = protocolo_cliente.recibir_vuelo()
            if estado == ESTADO_FIN_VUELOS:
                break

            logging.error(f'Vuelo recibido:  id vuelo: {vuelo.id_vuelo}   origen: {vuelo.origen}   destino: {vuelo.destino}  precio: {vuelo.precio} distancia: {vuelo.distancia} duracion: {vuelo.duracion} escalas: {vuelo.escalas}')


    def _recibir_aeropuertos(self, protocolo_cliente):
        while True:            
            estado, aeropuerto = protocolo_cliente.recibir_aeropuerto()
            if estado == ESTADO_FIN_AEROPUERTOS:
                break

            logging.error(f'Aeropuerto recibido:  id: {aeropuerto.id}   latitud: {aeropuerto.latitud}   longitud: {aeropuerto.longitud}')



    def run(self):
            self._colas.crear_cola('cola')
            try:
                client_sock, addr = self._server_socket.accept()
            except OSError:
                return

            protocolo_cliente = ProtocoloCliente(client_sock)  
            self._recibir_aeropuertos(protocolo_cliente)  
            self._recibir_vuelos(protocolo_cliente)


            client_sock.close()


            
    
        
        
        
           
