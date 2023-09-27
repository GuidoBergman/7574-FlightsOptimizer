import logging
import signal
from socket_comun import SocketComun, STATUS_ERR, STATUS_OK



class Client:
    def __init__(self, host, port):
        # Initialize server socket
        self._server_socket = SocketComun()
        self._server_socket.connect(host, port)
        signal.signal(signal.SIGTERM, self.sigterm_handler)

        
        

        

    def sigterm_handler(self, _signo, _stack_frame):
        logging.info('action: sigterm_received')
        self._server_socket.close()
        logging.info(f'action: close_server_socket | result: success')
        

    def run(self):
            self._server_socket.send('hola'.encode('utf-8'), 4)

                


            
    
        
        
        
           
