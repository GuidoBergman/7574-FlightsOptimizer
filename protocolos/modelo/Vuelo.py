class Vuelo:
    def __init__(self, id_vuelo: str, trayecto: str, precio: float, escalas: [str]):
        """
        Crea una instancia de la clase Vuelo.

        Args:
            id_vuelo (str): ID del vuelo.
            trayecto (str): Trayecto del vuelo (formato: "origen-destino").
            precio (float): Precio del vuelo.
            escalas (List[str]): Lista de segmentos del vuelo.
        """
        self.id_vuelo = id_vuelo
        self.trayecto = trayecto
        self.precio = precio
        self.escalas = escalas