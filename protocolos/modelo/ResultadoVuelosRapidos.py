from struct import unpack, pack, calcsize


STRING_ENCODING = 'utf-8'
FORMATO = '!32s7sH50s8s'


class ResultadoVuelosRapidos:
    def __init__(self, id: str, trayecto: str, escalas: str, duracion: str):
        self.id = id
        self.trayecto = trayecto
        self.escalas = escalas
        self.duracion = duracion


    def serializar(self):
        tamanio = calcsize(FORMATO)
        bytes = pack(FORMATO, 
            self.id.encode(STRING_ENCODING), self.trayecto.encode(STRING_ENCODING), 
            len(self.escalas), self.escalas.encode(STRING_ENCODING), 
            self.duracion.encode(STRING_ENCODING)
        )
        return tamanio, bytes

    def convertir_a_str(self):
        string = f'Tipo resultado: Filtro escalas vuelos rápidos  ID:{self.id}  Trayecto: {self.trayecto}  Escalas: {self.escalas}  Duracion: {self.duracion}'
        return string

    def deserializar(bytes):    
        id, trayecto, longitud_escalas, escalas, duracion = unpack(FORMATO, bytes)
        id = id.decode(STRING_ENCODING)
        trayecto = trayecto.decode(STRING_ENCODING)
        escalas = escalas[0:longitud_escalas].decode(STRING_ENCODING)
        duracion = duracion.decode(STRING_ENCODING)

        return ResultadoVuelosRapidos(id, trayecto, escalas, duracion)

    def calcular_tamanio():
        return calcsize(FORMATO)