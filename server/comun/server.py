from ast import Import
import chunk
import logging
import signal
from comun.handler import Handler
from comun.enviador_fin import EnviadorFin
from multiprocessing import Process, Manager
from socket_comun import SocketComun
from comun.sesioncliente import SesionCliente
from protocolo_resultados_servidor import ProtocoloResultadosServidor

from protocolo_enviar_heartbeat import ProtocoloEnviarHeartbeat, IDENTIFICADOR_SERVER
from socket_comun_udp import SocketComunUDP



EOF_MSG = 'EOF'

class Server:
    def __init__(self, port, listen_backlog, cant_handlers, cant_filtros_escalas,
    cant_filtros_distancia, cant_filtros_velocidad, cant_filtros_precio,
     cant_watchdogs, periodo_heartbeat, host_watchdog, port_watchdog):
        self._server_socket = SocketComun()
        self._server_socket.bind_and_listen('', port, listen_backlog)
        signal.signal(signal.SIGTERM, self.sigterm_handler)

        self._cant_handlers = cant_handlers
        self._cant_filtros_escalas = cant_filtros_escalas
        self._cant_filtros_distancia = cant_filtros_distancia
        self._cant_filtros_velocidad = cant_filtros_velocidad
        self._cant_filtros_precio = cant_filtros_precio
        self._hilos_cliente = []
        self._corriendo = True

        socket = SocketComunUDP()
        self._protocolo_heartbeat = ProtocoloEnviarHeartbeat(socket, host_watchdog, port_watchdog, cant_watchdogs,
            IDENTIFICADOR_SERVER, periodo_heartbeat)
 
        

        

    def sigterm_handler(self, _signo, _stack_frame):
        logging.error('sigterm_received')        
        self._corriendo = False
        if self._server_socket:
            self._server_socket.close()
            logging.info('Cerrando socket server')

        if self._protocolo_heartbeat:
            self._protocolo_heartbeat.cerrar()

        if self._handle_protocolo_heartbeat and self._protocolo_heartbeat.is_alive():
            self._handle_protocolo_heartbeat.terminate()
            self._handle_protocolo_heartbeat.join()
        
    def borrar_hilos_clientes(self):
        # Verificar los hilos que han terminado y unirlos
        borrar_hilos = []
        for thread in self._hilos_cliente:
            if not thread.is_alive():
                thread.join()
                borrar_hilos.append(thread)

        # Eliminar los hilos terminados de la lista
        for thread in borrar_hilos:
            self._hilos_cliente.remove(thread)


    # Run wrapper para el manejo de sigterm
    def run(self):
        try:
            logging.info('Iniciando servidor')
            self._handle_protocolo_heartbeat = Process(target=self._protocolo_heartbeat.enviar_heartbeats)  
            self._handle_protocolo_heartbeat.start()
            self._run()
        except:
            if self._handle_protocolo_heartbeat:
                self._handle_protocolo_heartbeat.terminate()
                self._handle_protocolo_heartbeat.join()



    def _crear_sesion_cliente(self, client_sock):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        try:
            sesion = SesionCliente(client_sock, self._cant_filtros_escalas ,self._cant_filtros_distancia,
                                   self._cant_filtros_velocidad, self._cant_filtros_precio)
            sesion.correr()
        except OSError as e:
            logging.error("action: receive_message | result: fail | error: {e}")
        finally:
            client_sock.close()
            


    def _run(self):
            while self._corriendo:
                self.borrar_hilos_clientes()
                client_sock, addr = self._server_socket.accept()
                hilo = Process(target=self._crear_sesion_cliente,
                                        args=(client_sock,))
                hilo.start()
                self._hilos_cliente.append(hilo)
            self._server_socket.close()            
            self._proceso_enviador.join()

           
