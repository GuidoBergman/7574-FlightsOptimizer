class ResumenPrecios:

    def __init__(self):
        self.maximo_precio = float('-inf')  # Inicializar con un valor mínimo
        self.suma_precios = 0.0
        self.total_vuelos = 0


    def actualizar(self, precio):
        if precio > self.maximo_precio:
            self.maximo_precio = precio
        self.suma_precios += precio
        self.total_vuelos += 1