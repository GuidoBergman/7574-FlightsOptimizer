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
        logging.error('sigterm_received')
        if self._server_socket:
            self._server_socket.close()
            logging.error('Cerrando socket server')
        
        if self._client_sock:
            self._client_sock.close()
            logging.error('Cerrando socket client')

        if self._vuelos:
            self._vuelos.close()
            logging.error('Cerrando la cola de vuelos')


        if self._proceso_enviador:
            self._proceso_enviador.terminate()
            self._proceso_enviador.join()
            logging.error('Cerrando proceos enviador')

        if self._procesos_handlers:
            for proceso in self._procesos_handlers:
                logging.error('Cerrando proceso handler')
                proceso.terminate()
                proceso.join()
        

    def _recibir_vuelos(self):
        while True:
            estado, vuelos_rec = self._protocolo_cliente.recibir_vuelos()
            if estado == ESTADO_FIN_VUELOS:
                logging.info(f'Se terminarón de recibir todos los vuelos')
                for i in range(self._cant_handlers):
                    self._vuelos.put(EOF_MSG)
                break
            
            logging.debug(f'Acción: recibir_vuelo | estado: OK | Vuelos recibidos:   {len(vuelos_rec)}')
            for vuelo in vuelos_rec:
                self._vuelos.put(vuelo)
            


    def _recibir_aeropuertos(self):
        while True:            
            estado, aeropuerto = self._protocolo_cliente.recibir_aeropuerto()
            if estado == ESTADO_FIN_AEROPUERTOS:
                self._protocoloDistancia.enviar_fin_aeropuertos()
                break
            
            logging.debug(f'Aeropuerto recibido:  id: {aeropuerto.id}   latitud: {aeropuerto.latitud}   longitud: {aeropuerto.longitud}')
            self._protocoloDistancia.enviar_aeropuerto(aeropuerto)


    # Run wrapper para el manejo de sigterm
    def run(self):
        try:
            self._run()
        except (ConnectionResetError, BrokenPipeError, OSError):
            return

    def _run(self):
            self._client_sock, addr = self._server_socket.accept()
            
            self._procesos_handlers = []
            with Manager() as manager:
                enviador_resultados = ProtocoloResultadosServidor()
                self._proceso_enviador = Process(target=enviador_resultados.iniciar, args=((self._client_sock,
                    self._cant_filtros_escalas, self._cant_filtros_distancia,
                    self._cant_filtros_velocidad, self._cant_filtros_precio
                )))
                self._proceso_enviador.start()

            
                self._vuelos = manager.Queue()
                for i in range(self._cant_handlers):
                    handler = Handler(self._cant_filtros_precio)
                    handler_process = Process(target=handler.run, args=((self._vuelos),))
                    handler_process.start()
                    self._procesos_handlers.append(handler_process)   

                self._protocolo_cliente = ProtocoloCliente(self._client_sock)  
                self._recibir_aeropuertos()  
                self._recibir_vuelos()

                
                for proceso in self._procesos_handlers:
                    proceso.join()

                enviador_fin = EnviadorFin(self._cant_filtros_escalas, self._cant_filtros_distancia,
                self._cant_filtros_precio)
                enviador_fin.enviar_fin_vuelos()

                self._proceso_enviador.join()

                self._client_sock.close()
                self._server_socket.close()

              

            
    
        
        
        
           
