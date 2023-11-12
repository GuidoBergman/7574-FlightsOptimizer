import chunk
import logging
import signal
from protocolo_cliente import ProtocoloCliente, ESTADO_FIN_VUELOS, ESTADO_FIN_AEROPUERTOS
from comun.handler import Handler
from comun.enviador_fin import EnviadorFin
from multiprocessing import Process, Manager
from protocolo_resultados_servidor import ProtocoloResultadosServidor

from protocolofiltroescalas import ProtocoloFiltroEscalas
from protocolofiltrodistancia import ProtocoloFiltroDistancia
from protocolofiltroprecio import ProtocoloFiltroPrecio

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
        
        self._protocoloEscalas = ProtocoloFiltroEscalas()
        self._protocoloDistancia = ProtocoloFiltroDistancia()
        self._protocoloPrecio = ProtocoloFiltroPrecio(cant_filtros_precio)

        
        

        

    def sigterm_handler(self, _signo, _stack_frame):
        logging.error('sigterm_received')
        if self._server_socket:
            self._server_socket.close()
            logging.info('Cerrando socket server')
        
        if self._client_sock:
            self._client_sock.close()
            logging.info('Cerrando socket client')

            
        
        if self._proceso_enviador:
            logging.info('Cerrando procesos enviador')
            self._proceso_enviador.terminate()
            self._proceso_enviador.join()

        if self._procesos_handlers:
            
            logging.info('Cerrando proceso handler')
            for proceso in self._procesos_handlers:
                proceso.terminate()
                proceso.join()
        

    def _recibir_vuelos(self):
        chunk_recibidos = 0
        logging.info('Recibiendo vuelos')
        while True:
            estado, vuelos_rec = self._protocolo_cliente.recibir_vuelos()
            if estado == ESTADO_FIN_VUELOS:
                logging.info(f'Se terminarón de recibir todos los vuelos')
                break
            
            # Muestra por logs los chunck recibidos
            logging.debug(f'Acción: recibir_vuelo | estado: OK | Nro chunck: { chunk_recibidos } | Vuelos recibidos:   {len(vuelos_rec)}')
            chunk_recibidos += 1
            if (chunk_recibidos % 100) == 1:
                logging.info(f'Lote de vuelos recibido: {chunk_recibidos}')
            
            # Manda los vuelos a los filtros
            self._protocoloEscalas.enviar_vuelos(vuelos_rec)
            #self._protocoloDistancia.enviar_vuelo(vuelos_rec)
            #self._protocoloPrecio.enviar_vuelo(vuelos_rec)
            


    def _recibir_aeropuertos(self):
        logging.info('Recibiendo aeropuertos')
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
            logging.info('Iniciando servidor')
            self._run()
        except (ConnectionResetError, BrokenPipeError, OSError):
            return

    def _run(self):
            self._client_sock, addr = self._server_socket.accept()
            
            enviador_resultados = ProtocoloResultadosServidor()
            self._proceso_enviador = Process(target=enviador_resultados.iniciar, args=((self._client_sock,
                    self._cant_filtros_escalas, self._cant_filtros_distancia,
                    self._cant_filtros_velocidad, self._cant_filtros_precio
                )))
            self._proceso_enviador.start()

            self._protocolo_cliente = ProtocoloCliente(self._client_sock)  
            self._recibir_aeropuertos()  
            self._recibir_vuelos()


            enviador_fin = EnviadorFin(self._cant_filtros_escalas, self._cant_filtros_distancia,
            self._cant_filtros_precio)
            enviador_fin.enviar_fin_vuelos()
            
            logging.info("Espera terminen los resultados resultados")
            self._proceso_enviador.join()

            self._client_sock.close()
            self._server_socket.close()

           
