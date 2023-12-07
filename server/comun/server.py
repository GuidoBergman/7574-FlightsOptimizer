from ast import Import
import logging
import os
import signal
from multiprocessing import Process
from socket_comun import SocketComun
from comun.sesioncliente import SesionCliente
import sys
import traceback

from protocolo_enviar_heartbeat import ProtocoloEnviarHeartbeat, IDENTIFICADOR_SERVER
from socket_comun_udp import SocketComunUDP



EOF_MSG = 'EOF'

class Server:
    def __init__(self, port, listen_backlog, cant_filtros_escalas,
    cant_filtros_distancia, cant_filtros_velocidad, cant_filtros_precio,
     cant_watchdogs, periodo_heartbeat, host_watchdog, port_watchdog):
        self._server_socket = SocketComun()
        self._server_socket.bind_and_listen('', port, listen_backlog)
        

        self._cant_filtros_escalas = cant_filtros_escalas
        self._cant_filtros_distancia = cant_filtros_distancia
        self._cant_filtros_velocidad = cant_filtros_velocidad
        self._cant_filtros_precio = cant_filtros_precio
        self._procesos_cliente = []
        self._corriendo = True

        signal.signal(signal.SIGTERM, self.sigterm_handler)

        socket = SocketComunUDP()
        self._protocolo_heartbeat = ProtocoloEnviarHeartbeat(socket, host_watchdog, port_watchdog, cant_watchdogs,
            IDENTIFICADOR_SERVER, periodo_heartbeat)
 
        

    def sigterm_handler(self, _signo, _stack_frame):
        logging.error('SIGTERM recibida (server)')
        self.cerrar()
        

    def cerrar(self):
        logging.error('Cerrando recursos')     
        self._corriendo = False
        if hasattr(self, '_server_socket'):
            self._server_socket.close()
            logging.info('Cerrando socket server')

        if hasattr(self, '_protocolo_heartbeat'):
            self._protocolo_heartbeat.cerrar()

        if hasattr(self, '_handle_protocolo_heartbeat'):
            if self._handle_protocolo_heartbeat.is_alive():
                self._handle_protocolo_heartbeat.terminate()
            self._handle_protocolo_heartbeat.join()

        self.borrar_procesos_clientes(True)
        
    def borrar_procesos_clientes(self, borrar_todos=False):
        # Verificar los hilos que han terminado y unirlos
        borrar_procesos = []
        for proceso in self._procesos_cliente:
            if not proceso.is_alive() and not borrar_todos:
                proceso.join()
                borrar_procesos.append(proceso)
            if borrar_todos:
                proceso.terminate()
                proceso.join()
                borrar_procesos.append(proceso)
            

        # Eliminar los hilos terminados de la lista
        for proceso in borrar_procesos:
            
            logging.info("Borrando proceso de cliente")
            self._procesos_cliente.remove(proceso)


    # Run wrapper para el manejo de sigterm


    def _flush_caidos(self):
        logging.info("Buscando clientes anteriores...")
        directorio = "/data"
        # Obtener la lista de archivos en el directorio
        archivos = [f for f in os.listdir(directorio) if os.path.isfile(os.path.join(directorio, f))]
        # Filtrar archivos que comienzan con "sesion" y extraer el número de cliente
        clientes = [archivo.replace("sesion", "") for archivo in archivos if archivo.startswith("sesion")]
        for cliente in clientes:
            logging.info(f"Encuentra el archivo de la sesion del cliente {cliente}")
            sesion = SesionCliente(None, self._cant_filtros_escalas ,self._cant_filtros_distancia,
                                   self._cant_filtros_velocidad, self._cant_filtros_precio, id_cliente = cliente)
            sesion._enviar_flush()
            




    def run(self):
        try:
            logging.info('Iniciando servidor')
            self._handle_protocolo_heartbeat = Process(target=self._protocolo_heartbeat.enviar_heartbeats)  
            self._handle_protocolo_heartbeat.start()
            self._flush_caidos()
            self._run()
        except Exception as e:
            logging.error(f'Ocurrió una excepción: {e}')
            if hasattr(sys, 'exception'):
                exc = sys.exception()         
                traceback.print_exception(exc, limit=2, file=sys.stdout)
            self.cerrar()



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
                self.borrar_procesos_clientes()
                client_sock, addr = self._server_socket.accept()
                hilo = Process(target=self._crear_sesion_cliente,
                                        args=(client_sock,))
                hilo.start()
                self._procesos_cliente.append(hilo)
            self._server_socket.close()            
            self._proceso_enviador.join()

           
