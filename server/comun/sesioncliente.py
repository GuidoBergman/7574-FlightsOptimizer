import uuid
import logging
import threading
from protocolofiltroescalas import ProtocoloFiltroEscalas
from protocolofiltrodistancia import ProtocoloFiltroDistancia
from protocolofiltroprecio import ProtocoloFiltroPrecio
from protocolo_resultados_servidor import ProtocoloResultadosServidor
from multiprocessing import Process
from socket_comun import SocketComun
from comun.enviador_fin import EnviadorFin
from protocolo_cliente import ProtocoloCliente, ESTADO_FIN_VUELOS, ESTADO_FIN_AEROPUERTOS

class SesionCliente:
    def __init__(self, _client_sock, cant_filtros_escalas,
        cant_filtros_distancia, cant_filtros_velocidad, cant_filtros_precio):        
        self._client_sock = _client_sock
        self._cant_filtros_escalas = cant_filtros_escalas
        self._cant_filtros_distancia = cant_filtros_distancia
        self._cant_filtros_velocidad = cant_filtros_velocidad
        self._cant_filtros_precio = cant_filtros_precio        
        
        guid = uuid.uuid4()
        self.id_cliente = str(guid).replace("-", "")
        
        logging.info(f'Inicia cliente {self.id_cliente}')
        self._protocoloEscalas = ProtocoloFiltroEscalas(self.id_cliente)
        self._protocoloPrecio = ProtocoloFiltroPrecio(cant_filtros_precio, self.id_cliente)
        self._protocoloDistancia = ProtocoloFiltroDistancia(self.id_cliente)
          
          
    def correr(self):
            enviador_resultados = ProtocoloResultadosServidor(self.id_cliente)
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
            enviador_fin.enviar_fin_vuelos(self.id_cliente)
            
            logging.info("Espera terminen los resultados resultados")
            self._proceso_enviador.join()
            self._client_sock.close()            
            logging.info(f'Termina el proceso cliente {self.id_cliente}')

    def _recibir_aeropuertos(self):
        logging.info('Recibiendo aeropuertos')
        while True:            
            estado, aeropuertos = self._protocolo_cliente.recibir_aeropuertos()
            if estado == ESTADO_FIN_AEROPUERTOS:
                self._protocoloDistancia.enviar_fin_aeropuertos(self.id_cliente)
                break
            logging.info(f'Aeropuertos recibidos: { self.id_cliente} total { len(aeropuertos)}')
            self._protocoloDistancia.enviar_aeropuertos(self.id_cliente, aeropuertos)
            
    def _recibir_vuelos(self):
        chunk_recibidos = 0
        logging.info('Recibiendo vuelos')
        while True:
            estado, vuelos_rec = self._protocolo_cliente.recibir_vuelos()
            if estado == ESTADO_FIN_VUELOS:
                logging.info(f'Se terminaron de recibir todos los vuelos')
                break
            
            # Muestra por logs los chunck recibidos
            logging.debug(f'Accion: recibir_vuelo | estado: OK | Nro chunck: { chunk_recibidos } | Vuelos recibidos:   {len(vuelos_rec)}')
            chunk_recibidos += 1
            if (chunk_recibidos % 100) == 1:
                logging.info(f'Cliente: {self.id_cliente} Lote de vuelos recibido: {chunk_recibidos}')
            
            # Manda los vuelos a los filtros
            #self._protocoloPrecio.enviar_vuelos(vuelos_rec)
            self._protocoloEscalas.enviar_vuelos(vuelos_rec)
            self._protocoloDistancia.enviar_vuelos(vuelos_rec)
            