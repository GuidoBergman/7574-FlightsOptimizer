import logging
import signal
from protocolo_cliente import ProtocoloCliente, ESTADO_FIN_VUELOS, ESTADO_FIN_AEROPUERTOS
from comun.handler import Handler
from comun.enviador_fin import EnviadorFin
from multiprocessing import Process, Manager
from protocolo_resultados_servidor import ProtocoloResultadosServidor

from protocolofiltrodistancia import ProtocoloFiltroDistancia
from socket_comun import SocketComun


EOF_MSG = 'EOF'

class Server:
    def __init__(self, port, listen_backlog, cant_handlers, cant_filtros_escalas,
    cant_filtros_distancia, cant_filtros_velocidad, cant_filtros_precio):
        # Initialize server socket
        self._server_socket = SocketComun()
        self._server_socket.bind_and_listen('', port, listen_backlog)
        signal.signal(signal.SIGTERM, self.sigterm_handler)
        self._protocoloDistancia = ProtocoloFiltroDistancia()

        self._cant_handlers = cant_handlers
        self._cant_filtros_escalas = cant_filtros_escalas
        self._cant_filtros_distancia = cant_filtros_distancia
        self._cant_filtros_velocidad = cant_filtros_velocidad
        self._cant_filtros_precio = cant_filtros_precio

        
        

        

    def sigterm_handler(self, _signo, _stack_frame):
        logging.info('action: sigterm_received')
        self._server_socket.close()
        logging.info(f'action: close_server_socket | result: success')
        

    def _recibir_vuelos(self, protocolo_cliente, vuelos):
        while True:
            logging.error(f'Acción: recibir_vuelo | estado: en curso')
            estado, vuelos_rec = protocolo_cliente.recibir_vuelos()
            if estado == ESTADO_FIN_VUELOS:
                logging.error(f'Acción: recibir_vuelo | estado: se terminarón de recibir todos los vuelos')
                for i in range(self._cant_handlers):
                    vuelos.put(EOF_MSG)
                break
            
            logging.info(f'Acción: recibir_vuelo | estado: OK | Vuelos recibidos:   {len(vuelos_rec)}')
            for vuelo in vuelos_rec:
                vuelos.put(vuelo)
            


    def _recibir_aeropuertos(self, protocolo_cliente):
        while True:            
            estado, aeropuerto = protocolo_cliente.recibir_aeropuerto()
            if estado == ESTADO_FIN_AEROPUERTOS:
                self._protocoloDistancia.enviar_fin_aeropuertos()
                break
            
            logging.info(f'Aeropuerto recibido:  id: {aeropuerto.id}   latitud: {aeropuerto.latitud}   longitud: {aeropuerto.longitud}')
            self._protocoloDistancia.enviar_aeropuerto(aeropuerto)



    def run(self):
            
            try:
                client_sock, addr = self._server_socket.accept()
            except OSError:
                return

           # socket_enviar, socket_recibir = client_sock.split()
            
            

            procesos_handlers = []
            with Manager() as manager:
                enviador_resultados = ProtocoloResultadosServidor()
                proceso_enviador = Process(target=enviador_resultados.iniciar, args=((client_sock,
                    self._cant_filtros_escalas, self._cant_filtros_velocidad,
                    self._cant_filtros_distancia, self._cant_filtros_precio
                )))
                proceso_enviador.start()

            
                vuelos = manager.Queue()
                for i in range(self._cant_handlers):
                    handler = Handler()
                    handler_process = Process(target=handler.run, args=((vuelos),))
                    handler_process.start()
                    procesos_handlers.append(handler_process)   

                protocolo_cliente = ProtocoloCliente(client_sock)  
                self._recibir_aeropuertos(protocolo_cliente)  
                self._recibir_vuelos(protocolo_cliente, vuelos)

                
                for proceso in procesos_handlers:
                    proceso.join()

                enviador_fin = EnviadorFin(self._cant_filtros_escalas, self._cant_filtros_distancia,
                self._cant_filtros_precio)
                enviador_fin.enviar_fin_vuelos()
                protocolo_cliente.cerrar()
                proceso_enviador.join()
              

            
    
        
        
        
           
