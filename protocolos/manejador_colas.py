#!/usr/bin/env python3
import pika
import os

class ManejadorColas:
    def __init__(self, host):
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host))
        self._channel = connection.channel()
        self._channel.basic_qos(prefetch_count=1)


    def crear_cola(self, nombre_cola):
        self._channel.queue_declare(queue=nombre_cola)


    def consumir_mensajes(self, nombre_cola, callback_function):
       self.callback_function = callback_function
       self._channel.basic_consume(queue=nombre_cola, on_message_callback=self._callback_wrapper)
       self._channel.start_consuming() 
       

    def _callback_wrapper(self, channel, method, properties, body):
        self.callback_function(body)
        channel.basic_ack(delivery_tag=method.delivery_tag)

        

    def prepararconsumir_mensajes2(self, nombre_cola, callback_function):
       self.callback_function2 = callback_function
       self._channel.basic_consume(queue=nombre_cola, on_message_callback=self._callback_wrapper2)
       

    def _callback_wrapper2(self, channel, method, properties, body):
        self.callback_function2(body)
        channel.basic_ack(delivery_tag=method.delivery_tag)
        

    def enviar_mensaje(self, nombre_cola, mensaje):
        self._channel.basic_publish(exchange='', routing_key=nombre_cola, body=mensaje)



    def cerrar(self):
        self._connection.close()

