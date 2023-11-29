from almacenador import Almacenador
import logging

class Recuperador():
    def __init__(self):
        self._almacenador = Almacenador()


    def almacenar(self, id_cliente, body_msj, contenido_a_peristir=None):
        if contenido_a_peristir:
            contenido_a_peristir = str(hash(body_msj)) + ',' + str(contenido_a_peristir)
        else:
            contenido_a_peristir = str(hash(body_msj))
        self._almacenador.guardar_linea(id_cliente, contenido_a_peristir)

    def recuperar_siguiente_linea(self):
        for nombre_archivo, linea in self._almacenador.obtener_siguiente_linea():
            linea = linea.split(',')
            hash_mensaje = linea[0]
            logging.login(f'Encontré el mensaje procesado {hash_mensaje}')

            if len(linea) > 1:
                yield nombre_archivo, linea[1:]

    def es_duplicado(self, id_cliente, body_msj):
        return False

    def cerrar(self):
        self._almacenador.cerrar()