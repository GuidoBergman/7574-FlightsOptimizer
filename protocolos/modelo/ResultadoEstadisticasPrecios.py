from struct import unpack, pack, calcsize


STRING_ENCODING = 'utf-8'
FORMATO = '!32sfH'

class ResultadoEstadisticaPrecios:
    def __init__(self, id_vuelo: str, trayecto: str, escalas: str, duracion: str):
        self.id_vuelo = id_vuelo
        self.trayecto = trayecto
        self.escalas = escalas
        self.duracion = duracion