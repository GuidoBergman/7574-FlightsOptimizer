
class PromedioCliente:
    
    def __init__(self, id_cliente, promedio, cantidad):
       self.id_cliente = id_cliente
       self.promedio = promedio
       self.cantidad = cantidad
       self.recibidos = 1
    
    def agregar(self, promedio, cantidad):
        if cantidad > 0:
            parte_actual = self.cantidad / (self.cantidad + cantidad)
            parte_nueva = cantidad / (self.cantidad + cantidad)
            npromedio = (self.promedio * parte_actual) + (promedio * parte_nueva)
            self.promedio = npromedio
            self.cantidad += cantidad
        self.recibidos += 1