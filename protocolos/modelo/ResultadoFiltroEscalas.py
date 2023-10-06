from struct import unpack, pack, calcsize


STRING_ENCODING = 'utf-8'
FORMATO = '!32s7sfh50s'


class ResultadoFiltroEscalas:
    def __init__(self, id: str, trayecto: str, precio: float, escalas: str):
        self.id = id
        self.trayecto = trayecto
        self.precio = precio
        self.escalas = escalas

    def serializar(self):
        tamanio = calcsize(FORMATO)
        bytes = pack(FORMATO, 
            self.id.encode(STRING_ENCODING), self.trayecto.encode(STRING_ENCODING), 
            self.precio, len(self.escalas), self.escalas.encode(STRING_ENCODING)
        )
        return tamanio, bytes

    def convertir_a_str(self):
        string = f'Tipo resultado: Filtro escalas  ID:{self.id}  Trayecto: {self.trayecto}  Precio: {self.precio}  Escalas: {self.escalas}'
        return string

    def deserializar(bytes):    
        id, trayecto, precio, longitud_escalas, escalas = unpack(FORMATO, bytes)
        id = id.decode(STRING_ENCODING)
        escalas = escalas[0:longitud_escalas].decode(STRING_ENCODING)

        return ResultadoFiltroEscalas(id, trayecto, precio, escalas)

    def calcular_tamanio():
        return calcsize(FORMATO)

    
