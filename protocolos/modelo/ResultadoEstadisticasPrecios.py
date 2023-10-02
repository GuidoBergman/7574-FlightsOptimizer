class ResultadoEstadisticaPrecios:
    def __init__(self, id_vuelo: str, trayecto: str, escalas: [str], duracion: str):
        self.id_vuelo = id_vuelo
        self.trayecto = trayecto
        self.escalas = escalas
        self.duracion = duracion