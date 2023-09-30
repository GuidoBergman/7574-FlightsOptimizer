from filtros.filtro_precio.comun.resumen_precios import ResumenPrecios


class PrecioReduce:

    def __init__(self):
        self.resumenes_precios = []

        
    def agregar_resumen(self, resumen: ResumenPrecios):
        self.resumenes_precios.append(resumen)
        

    def calcular_promedio(self):
        total_precios = sum([resumen.suma_precios for resumen in self.resumenes_precios])
        total_vuelos = sum([resumen.total_vuelos for resumen in self.resumenes_precios])
        return total_precios / total_vuelos if total_vuelos > 0 else 0.0

    def enviar(estaditica):
        print(estaditica)

    def enviar_query(self):
        promedio = self.calcular_promedio()

        for resumen in self.resumenes_precios:
            if resumen.maximo_precio > promedio:
                estadisticas = EstadisticasPrecio(resumen.trayecto, resumen.maximo_precio, promedio)
                self.enviar(estadisticas)

    