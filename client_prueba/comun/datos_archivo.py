
import logging
from venv import logger


class DatosArchivo:
    def __init__(self, nombre_archivo, cant_result_velocidad, cant_result_distancia, cant_result_precio, cant_result_escalas):
        self.nombre_archivo = nombre_archivo
        self.cant_result_velocidad = cant_result_velocidad
        self.cant_result_distancia = cant_result_distancia
        self.cant_result_precio = cant_result_precio
        self.cant_result_escalas = cant_result_escalas

        self.cant_result_rec_velocidad = 0
        self.cant_result_rec_distancia = 0
        self.cant_result_rec_precio = 0
        self.cant_result_rec_escalas = 0
    
    def validar(self):
        error = False
        if (self.cant_result_rec_velocidad != self.cant_result_velocidad):
            logging.error(f"VELOCIDAD: Se esperaban {self.cant_result_velocidad}, se recibieron: {self.cant_result_rec_velocidad}")
            error = True
        if (self.cant_result_rec_distancia != self.cant_result_distancia):
            logging.error(f"DISTANCIA: Se esperaban {self.cant_result_distancia}, se recibieron: {self.cant_result_rec_distancia}")
            error = True
        if (self.cant_result_rec_escalas!= self.cant_result_escalas):
            logging.error(f"ESCALAS: Se esperaban {self.cant_result_escalas}, se recibieron: {self.cant_result_rec_escalas}")
            error = True
        if (self.cant_result_rec_precio != self.cant_result_precio):
            logging.error(f"PRECIO: Se esperaban {self.cant_result_precio}, se recibieron: {self.cant_result_rec_precio}")
            error = True
        if (error):
            raise Exception("RESULTADOS INCORRECTOS")
