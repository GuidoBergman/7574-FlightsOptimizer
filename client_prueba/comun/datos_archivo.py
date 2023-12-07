
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
        
    def contar_lineas(self, archivo):
        try:
            with open(archivo, 'r') as file:
                lineas = file.readlines()
                return len(lineas)
        except FileNotFoundError:
            print(f"El archivo '{archivo}' no existe.")
            return 0
        except Exception as e:
            print(f"Error al intentar abrir el archivo '{archivo}': {e}")
            return 0
    
    def validar(self):
        archivo_ResultadoFiltroDistancia = '/resultados/ResultadoFiltroDistancia.txt'
        archivo_ResultadoFiltroEscalas = '/resultados/ResultadoFiltroEscalas.txt'    
        archivo_ResultadoVuelosRapidos = '/resultados/ResultadoVuelosRapidos.txt'    
        archivo_ResultadoEstadisticaPrecios = '/resultados/ResultadoEstadisticaPrecios.txt'
        
        self.cant_result_rec_velocidad = self.contar_lineas(archivo_ResultadoVuelosRapidos)
        self.cant_result_rec_distancia = self.contar_lineas(archivo_ResultadoFiltroDistancia)
        self.cant_result_rec_precio = self.contar_lineas(archivo_ResultadoEstadisticaPrecios)
        self.cant_result_rec_escalas = self.contar_lineas(archivo_ResultadoFiltroEscalas)
        error = False
        if (self.cant_result_rec_velocidad != self.cant_result_velocidad):
            logging.error(f"VELOCIDAD: Se esperaban {self.cant_result_velocidad}, se recibieron: {self.cant_result_rec_velocidad}")
            error = True
        if (self.cant_result_rec_distancia != self.cant_result_distancia):
            logging.error(f"DISTANCIA: Se esperaban {self.cant_result_distancia}, se recibieron: {self.cant_result_rec_distancia}")
            error = True
        if (self.cant_result_rec_precio != self.cant_result_precio):
            logging.error(f"PRECIO: Se esperaban {self.cant_result_precio}, se recibieron: {self.cant_result_rec_precio}")
            error = True
        if (self.cant_result_rec_escalas!= self.cant_result_escalas):
            logging.error(f"ESCALAS: Se esperaban {self.cant_result_escalas}, se recibieron: {self.cant_result_rec_escalas}")
            error = True
        if (error):
           raise Exception("RESULTADOS INCORRECTOS")
        logging.info("Todos los resultados se recibieron correctamente")