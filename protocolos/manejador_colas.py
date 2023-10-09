#!/usr/bin/env python3
import pika
import os

class ManejadorColas:
    def __init__(self, host):
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host))
        self._channel = connection.channel()
        self._channel.basic_qos(prefetch_count=1)

        self._consumer_tags = {}


    def crear_cola(self, nombre_cola):
        self._channel.queue_declare(queue=nombre_cola)


    def consumir_mensajes(self, nombre_cola, callback_function):
       self.callback_function = callback_function
       self._consumer_tags[nombre_cola] = self._channel.basic_consume(queue=nombre_cola, 
        on_message_callback=self._callback_wrapper)
       self._channel.start_consuming() 

    def dejar_de_consumir(self, nombre_cola):
        consumer_tag = self._consumer_tags[nombre_cola]
        self._channel.basic_cancel(consumer_tag) 

       

    def crear_cola_por_topico(self, nombre_cola):
        self._channel.exchange_declare(exchange=nombre_cola, exchange_type='direct')
    
    def consumir_mensajes_por_topico(self, nombre_cola, callback_function, topico):
       resultado = self._channel.queue_declare(queue='')
       nombre_cola_anonima = resultado.method.queue
       self._channel.queue_bind(exchange=nombre_cola, queue=nombre_cola_anonima, routing_key=str(topico))

       self.callback_function = callback_function
       self._consumer_tags[nombre_cola] =  self._channel.basic_consume(queue=nombre_cola_anonima, 
        on_message_callback=self._callback_wrapper)
       self._channel.start_consuming() 

    def enviar_mensaje_por_topico(self, nombre_cola, mensaje, topico):
        self._channel.basic_publish(exchange=nombre_cola, routing_key=str(topico), body=mensaje)


    def _callback_wrapper(self, channel, method, properties, body):
        self.callback_function(body)
        channel.basic_ack(delivery_tag=method.delivery_tag)

        

    def prepararconsumir_mensajes2(self, nombre_cola, callback_function):
       self.callback_function2 = callback_function
       self._consumer_tags[nombre_cola] = self._channel.basic_consume(queue=nombre_cola, 
        on_message_callback=self._callback_wrapper2)
       

    def _callback_wrapper2(self, channel, method, properties, body):
        self.callback_function2(body)
        channel.basic_ack(delivery_tag=method.delivery_tag)
        

    def enviar_mensaje(self, nombre_cola, mensaje):
        self._channel.basic_publish(exchange='', routing_key=nombre_cola, body=mensaje)



    def cerrar(self):
        self._channel.close()

