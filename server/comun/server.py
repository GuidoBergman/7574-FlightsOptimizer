import logging
import signal
from socket_comun import SocketComun, STATUS_ERR, STATUS_OK



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
        

    def run(self):
            while True:
                logging.info('action: accept_connections | result: in_progress')
                try:
                    client_sock, addr = self._server_socket.accept()
                except OSError:
                    break
                
                _, msg, _ = client_sock.receive(4)
                print(msg.decode('utf-8'))

                client_sock.close()


            
    
        
        
        
           
