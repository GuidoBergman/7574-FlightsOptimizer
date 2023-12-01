from modelo.ResultadoEstadisticasPrecios import ResultadoEstadisticaPrecios
import logging


class ListaPrecios(dict):
    def __init__(self, id_cliente):
        self.id_cliente = id_cliente
        self.prom_cant = (0, 0)
        self.trayectos = {}
        self.trayectos_max = {}

    def vuelos_x_trayecto(self, vuelos):
        devolver = {}
        for vuelo in vuelos:
            trayecto = f'{vuelo.origen}-{vuelo.destino}'
            if trayecto not in self.archivos_por_trayecto:
                 devolver[trayecto] = [vuelo.precio]
            else:
                devolver[trayecto].append(vuelo.precio)
        return devolver
    
    
    def agregar(self, prom_cant, agregar):
        promedio, cantidad = prom_cant
        promedio_agregar, cantidad_agregar = agregar
        if cantidad > 0:
            parte_actual = cantidad / (cantidad_agregar + cantidad)
            parte_nueva = cantidad_agregar / (cantidad_agregar + cantidad)
            npromedio = (promedio * parte_actual) + (promedio_agregar * parte_nueva)
            toRet = (npromedio, cantidad + cantidad_agregar)
        logging.info(f"Nuevo promedio {toRet} ")
        return toRet
        
    def agregar_vuelos(self, vuelos):
        contenido_a_persisitir = 'V'       
        x_trayecto = self.vuelos_x_trayecto(vuelos)
        total = 0
        cantidad = 0
        for trayecto, precios in x_trayecto:
            contenido_a_persisitir += f',{trayecto},{len(precios)}'
            cantidad += len(precios)
            total += sum(precios)
            for precio in precios:
                contenido_a_persisitir += f',{precio}'
        self.prom_cant = self.agregar(self.prom_cant, (total / cantidad, cantidad))
        return contenido_a_persisitir
    
    def recuperar_promedios(self, valores):
        i = 1
        while i < len(valores):
            trayecto = valores[i]
            cantidad = int(valores[i + 1])
            i += 2
            suma = 0
            for i in range(i, i + cantidad - 1):
                suma += float(valores[i])
            i += cantidad
            self.prom_cant = self.agregar(self.prom_cant, (suma / cantidad, cantidad))

    def procesar_linea(self, promedio, valores):
        nuevos_trayectos = {}
        i = 1
        
        while i < len(valores):
            trayecto = valores[i]
            if not trayecto in self.trayectos_max:
                self.trayectos_max[trayecto] = 0
            cantidad = int(valores[i + 1])
            cantidad_sobrepromedio = 0
            i += 2
            suma = 0
            for i in range(i, i + cantidad - 1):
                valor = float(valores[i])
                if valor > promedio: 
                    suma += valor
                    cantidad_sobrepromedio += 1
                if self.trayectos_max[trayecto] < valor:
                    self.trayectos_max[trayecto] = valor
            i += cantidad
            if cantidad_sobrepromedio > 0:
                nuevos_trayectos[trayecto] = (suma / cantidad_sobrepromedio, cantidad_sobrepromedio)
            
        for (trayecto, prom_tot) in nuevos_trayectos:
            if trayecto in self.trayectos:
                self.trayectos[trayecto] = self.agregar(self.trayectos[trayecto], prom_tot)
            else:
                self.trayectos[trayecto] = prom_tot
        
        
    def get_resultados(self):
        resultados = []
        for trayecto in self.trayectos:
            promedio, total = self.trayectos[trayecto]
            resultados.append(ResultadoEstadisticaPrecios(trayecto, promedio, self.trayectos_max[trayecto]))
        return resultados