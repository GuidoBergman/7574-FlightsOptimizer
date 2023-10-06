from struct import unpack, pack, calcsize


STRING_ENCODING = 'utf-8'
FORMATO = '!32s7sH'


class ResultadoFiltroDistancia:
    def __init__(self, id: str, trayecto: str, distancia_total: int):
        self.id = id
        self.trayecto = trayecto
        self.distancia_total = distancia_total

    def serializar(self):
        tamanio = calcsize(FORMATO)
        bytes = pack(FORMATO, 
            self.id.encode(STRING_ENCODING), self.trayecto.encode(STRING_ENCODING), 
            self.distancia_total
        )
        return tamanio, bytes

    def convertir_a_str(self):
        string = f'Tipo resultado: Filtro distancia  ID:{self.id}  Trayecto: {self.trayecto}  Distancia total: {self.distancia_total}'
        return string

    def deserializar(bytes):    
        id, trayecto, distancia_total = unpack(FORMATO, bytes)
        id = id.decode(STRING_ENCODING)
        trayecto = trayecto.decode(STRING_ENCODING)

        return ResultadoFiltroDistancia(id, trayecto, distancia_total)

    def calcular_tamanio():
        return calcsize(FORMATO)
