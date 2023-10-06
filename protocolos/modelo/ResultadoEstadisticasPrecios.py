from struct import unpack, pack, calcsize


STRING_ENCODING = 'utf-8'
FORMATO = '!7sff'

class ResultadoEstadisticaPrecios:
    def __init__(self, trayecto: str, precio_promedio: float, precio_maximo: float):
        self.trayecto = trayecto
        self.precio_promedio = precio_promedio
        self.precio_maximo = precio_maximo
        

    def serializar(self):
        tamanio = calcsize(FORMATO)
        bytes = pack(FORMATO, 
            self.trayecto.encode(STRING_ENCODING), 
            self.precio_promedio, self.precio_maximo
        )
        return tamanio, bytes

    def convertir_a_str(self):
        string = f'Tipo resultado: Estadisticas precios   Trayecto: {self.trayecto}  Precio promedio: {self.precio_promedio} Precio promedio: {self.precio_maximo}'
        return string

    def deserializar(bytes):    
        trayecto, precio_prmedio, precio_maximo = unpack(FORMATO, bytes)
        trayecto = trayecto.decode(STRING_ENCODING)

        return ResultadoEstadisticaPrecios(trayecto, precio_prmedio, precio_maximo)

    def calcular_tamanio():
        return calcsize(FORMATO)
