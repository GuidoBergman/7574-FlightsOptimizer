from almacenador import Almacenador
import logging
from collections import OrderedDict 

CANT_MAX_MENSAJES_PROCESADOS = 10000

class Recuperador():
    def __init__(self):
        self._almacenador = Almacenador()
        self._mensajes_procesados = OrderedDict()

    def eliminar_archivo(self, nombre_archivo):
        self._almacenador.eliminar_archivo(nombre_archivo)
        

    def almacenar(self, id_cliente, body_msj, contenido_a_peristir=None):
        hash_msj = str(hash(body_msj))
        if contenido_a_peristir:
            contenido_a_peristir = hash_msj + ',' + str(contenido_a_peristir)
        else:
            contenido_a_peristir = hash_msj
        self._almacenador.guardar_linea(id_cliente, contenido_a_peristir)
        logging.debug(f'Guardo el hash: {hash_msj} que corresponde al mensaje {body_msj}')
        self._guardar_hash_mensaje(hash_msj)
        
    
    def _guardar_hash_mensaje(self, hash_msj):
        self._mensajes_procesados[hash_msj] = True
        while len(self._mensajes_procesados) > CANT_MAX_MENSAJES_PROCESADOS:
            self._mensajes_procesados.popitem(last=False)

    def recuperar_siguiente_linea(self):
        for nombre_archivo, linea in self._almacenador.obtener_siguiente_linea():
            linea = linea.split(',')
            hash_msj = linea[0]
            logging.debug(f'Encontré el mensaje procesado {hash_msj}')
            self._guardar_hash_mensaje(hash_msj)
            if len(linea) > 1:
                yield nombre_archivo, linea[1:]


    def obtener_siguiente_linea_cliente(self, id_cliente):
        for linea in self._almacenador.obtener_siguiente_linea_cliente(id_cliente):
            linea = linea.split(',')
            if len(linea) > 1:
                yield linea[1:]

    def es_duplicado(self, id_cliente, body_msj):
        hash_msj = str(hash(body_msj))
        if hash_msj in self._mensajes_procesados: # Borrame
             logging.debug(f'Es duplicado porque tiene el hash: {hash_msj}') 
        return hash_msj in self._mensajes_procesados

    def cerrar(self):
        self._almacenador.cerrar()