import uuid
import logging
import threading
from protocolofiltroescalas import ProtocoloFiltroEscalas
from protocolofiltrodistancia import ProtocoloFiltroDistancia
from protocolofiltroprecio import ProtocoloFiltroPrecio
from protocolo_resultados_servidor import ProtocoloResultadosServidor
from multiprocessing import Process
from socket_comun import SocketComun
from protocolo_cliente import ProtocoloCliente, ESTADO_FIN_VUELOS, ESTADO_FIN_AEROPUERTOS, STATUS_ERR
import signal
import sys
import traceback

class SesionCliente:
    def __init__(self, _client_sock, cant_filtros_escalas,
        cant_filtros_distancia, cant_filtros_velocidad, cant_filtros_precio, id_cliente = None):
        
        if id_cliente is not None:
            logging.info("Creando sesion del cliente {id_cliente}")
            self.id_cliente = id_cliente
        else:
            guid = uuid.uuid4()
            self.id_cliente = str(guid).replace("-", "")
        
        signal.signal(signal.SIGTERM, self.sigterm_handler)        
        self._client_sock = _client_sock
        self._cant_filtros_escalas = cant_filtros_escalas
        self._cant_filtros_distancia = cant_filtros_distancia
        self._cant_filtros_velocidad = cant_filtros_velocidad
        self._cant_filtros_precio = cant_filtros_precio        
        
        #Define las consultas que va a realizar el cliente
        self._protocolos = []
        self._protocolos_con_aeropuerto = []
        self._protocolos.append(ProtocoloFiltroEscalas(cant_filtros_escalas, self.id_cliente))
        self._protocolos.append(ProtocoloFiltroPrecio(cant_filtros_escalas, self.id_cliente))
        
        # El protocolo distancia lo arreglo en los dos vectores de protocolos
        protDistancia = ProtocoloFiltroDistancia(cant_filtros_escalas, self.id_cliente)
        self._protocolos.append(protDistancia)
        self._protocolos_con_aeropuerto.append(protDistancia)
  
    def correr(self):
        try:    
            
            logging.info(f"Inicia proceso para cliente: {self.id_cliente}")            
            enviador_resultados = ProtocoloResultadosServidor(self.id_cliente)
            self._proceso_enviador = Process(target=enviador_resultados.iniciar, args=((self._client_sock,
                    self._cant_filtros_escalas, self._cant_filtros_distancia,
                    self._cant_filtros_velocidad, self._cant_filtros_precio
                )))            
            self._proceso_enviador.start()
            self._protocolo_cliente = ProtocoloCliente(self._client_sock)  
            self._recibir_aeropuertos()  
            self._recibir_vuelos()
            self._enviar_fin_vuelos()
            logging.info("Espera terminen los resultados")
            self._proceso_enviador.join()
            self._client_sock.close()            
            logging.info(f'Termina el proceso cliente {self.id_cliente}')
        except Exception as e:
            logging.error(f'Ocurrió una excepción: {e}')
            if hasattr(sys, 'exception'):
                exc = sys.exception()
                traceback.print_tb(exc.__traceback__, limit=1, file=sys.stdout)          
                traceback.print_exception(exc, limit=2, file=sys.stdout)
            self._enviar_flush()
          
    def _recibir_aeropuertos(self):
        logging.info('Recibiendo aeropuertos')
        while True:            
            estado, aeropuertos = self._protocolo_cliente.recibir_aeropuertos()
            if estado == ESTADO_FIN_AEROPUERTOS:
                for prot in self._protocolos_con_aeropuerto:
                    prot.enviar_fin_aeropuertos(self.id_cliente)
                break
            elif estado == STATUS_ERR:
                logging.error(f'Error al recibir aeropuerto')
                break
            logging.info(f'Aeropuertos recibidos: { self.id_cliente} total { len(aeropuertos)}')
            for prot in self._protocolos_con_aeropuerto:
                    prot.enviar_aeropuertos(self.id_cliente, aeropuertos)
            
            
    def _enviar_flush(self):
        logging.info(f"Enviando FLUSH para cliente {self.id_cliente}")
        for prot in self._protocolos:
            prot.enviar_flush(self.id_cliente)
        
    def _recibir_vuelos(self):
        chunk_recibidos = 0
        logging.info('Recibiendo vuelos')
        while True:
            estado, vuelos_rec = self._protocolo_cliente.recibir_vuelos()
            if estado == ESTADO_FIN_VUELOS:
                logging.info(f'Se terminaron de recibir todos los vuelos')
                break
            elif estado == STATUS_ERR:
                logging.error(f'Error al recibir vuelo')
                break

            # Muestra por logs los chunck recibidos
            logging.debug(f'Accion: recibir_vuelo | estado: OK | Nro chunck: { chunk_recibidos } | Vuelos recibidos:   {len(vuelos_rec)}')
            chunk_recibidos += 1
            if (chunk_recibidos % 100) == 1:
                logging.info(f'Cliente: {self.id_cliente} Lote de vuelos recibido: {chunk_recibidos}')
            
            # Manda los vuelos a los filtros
            #self._protocoloPrecio.enviar_vuelos(vuelos_rec)
            for prot in self._protocolos:
                prot.enviar_vuelos(self.id_cliente, vuelos_rec)
         
            
    def _enviar_fin_vuelos(self):
        logging.info(f"Enviando FIN VUELOS para cliente {self.id_cliente}")
        for prot in self._protocolos:
            prot.enviar_fin_vuelos(self.id_cliente)
         
                
    def sigterm_handler(self, _signo, _stack_frame):
        logging.error('SIGTERM recibida (sesión cliente)')
        self.cerrar()

    def cerrar(self):
        logging.info('Cerrando recursos (sesión cliente)')
        
        self._client_sock.close()
        for prot in self._protocolos:
            prot.cerrar()
        
        if hasattr(self, '_proceso_enviador'):
            self._proceso_enviador.terminate()
            self._proceso_enviador.join()

        if hasattr(self, '_protocolo_cliente'):
            self._protocolo_cliente.cerrar()