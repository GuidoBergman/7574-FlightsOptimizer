class Vuelo:
    def __init__(self, id_vuelo: str, origen: str, destino: str, 
        precio: float, escalas: str, duracion: str, distancia: int):
        
        # Crea una instancia de la clase Vuelo.
        
        self.id_vuelo = id_vuelo
        self.origen = origen
        self.destino = destino
        self.precio = precio
        self.escalas = escalas
        self.duracion = duracion
        self.distancia = distancia