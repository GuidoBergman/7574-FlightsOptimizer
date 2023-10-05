from struct import unpack, pack, calcsize


STRING_ENCODING = 'utf-8'
FORMATO = '!c32s7sfh50s'

class ResultadoFiltroEscalas:
    def __init__(self, id: str, trayecto: str, precio: float, escalas: str):
        self.id = id
        self.trayecto = trayecto
        self.precio = precio
        self.escalas = escalas
