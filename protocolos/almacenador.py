import os
from os import listdir
from os.path import isfile, join
import logging

BASEPATH_ARCHIVOS = 'data/'
EXTENCION = '.csv'


class Almacenador:
    def __init__(self):
        self._archivos = {}

        nombres_archivos = [f for f in listdir(BASEPATH_ARCHIVOS) if isfile(join(BASEPATH_ARCHIVOS, f))]
        for nombre_archivo in nombres_archivos:
            if nombre_archivo.endswith(EXTENCION):
                logging.debug(f'Encontré el archivo {nombre_archivo}')
                nombre_archivo = nombre_archivo.rstrip(EXTENCION)
                self._archivos[nombre_archivo] = open(join(BASEPATH_ARCHIVOS, nombre_archivo + EXTENCION), 'a+')


    def guardar_linea(self, nombre_archivo, contenido):
        if nombre_archivo not in self._archivos:
            self._archivos[nombre_archivo] = open(join(BASEPATH_ARCHIVOS, str(nombre_archivo) + EXTENCION), 'a+')

        self._archivos[nombre_archivo].write(str(contenido) + '\n')        


    def obtener_siguiente_linea_cliente(self, nombre_archivo):
        logging.info(f'Recuperando archivo {nombre_archivo}')
        with open(join(BASEPATH_ARCHIVOS, str(nombre_archivo) + EXTENCION), 'r') as archivo:
            for linea in archivo:
                logging.debug(f'Linea encontrada: {linea}')
                linea = linea.rstrip("\n")               
                yield nombre_archivo, linea
        logging.info(f'Termino la recuperación del archivo {nombre_archivo}')

    def obtener_siguiente_linea(self):
        for nombre_archivo in self._archivos.keys():
            logging.info(f'Recuperando archivo {nombre_archivo}')
            with open(join(BASEPATH_ARCHIVOS, str(nombre_archivo) + EXTENCION), 'r') as archivo:
                for linea in archivo:
                    logging.debug(f'Linea encontrada: {linea}')
                    linea = linea.rstrip("\n")               
                    yield nombre_archivo, linea
                logging.info(f'Termino la recuperación del archivo {nombre_archivo}')

    def eliminar_archivo(self, nombre_archivo):
        try:
            self._archivos[nombre_archivo].close()
            os.remove(join(BASEPATH_ARCHIVOS, str(nombre_archivo) + EXTENCION))
            del self._archivos[nombre_archivo]
        except (FileNotFoundError, KeyError):
            logging.error(f'El nombre de archivo {nombre_archivo} es invalido')

    def cerrar(self):
        for archivo in self._archivos.values():
            archivo.close()