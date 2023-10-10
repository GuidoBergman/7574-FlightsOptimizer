#!/usr/bin/env python3
import logging
import pika
import os

class Wrapper:
    def __init__(self, callback_function):
        self.callback_function = callback_function        

    def funcion_wrapper(self, channel, method, properties, body):
        self.callback_function(body)
        channel.basic_ack(delivery_tag=method.delivery_tag)

class ManejadorColas:
    def __init__(self, host):
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host))
        self._channel = connection.channel()
        self._channel.basic_qos(prefetch_count=1)
        self._consumer_tags = {}
        self._wrapers = {}

    def crear_cola(self, nombre_cola):
        self._channel.queue_declare(queue=nombre_cola)
    
    def vincular_wrapper(self, nombre_cola, callback_function, nombre_dic = ""):
       if (nombre_dic == ""):
            nombre_dic=nombre_cola
       wr = Wrapper(callback_function)
       self._consumer_tags[nombre_dic] = self._channel.basic_consume(queue=nombre_cola, 
        on_message_callback=wr.funcion_wrapper)
       self._wrapers[nombre_dic] = wr
        

    def consumir_mensajes(self, nombre_cola, callback_function):
       self.vincular_wrapper(nombre_cola, callback_function)

    def consumir_mensajes_por_topico(self, nombre_cola, callback_function, topico):
       resultado = self._channel.queue_declare(queue='')
       nombre_cola_anonima = resultado.method.queue
       self._channel.queue_bind(exchange=nombre_cola, queue=nombre_cola_anonima, routing_key=str(topico))       
       self.vincular_wrapper(nombre_cola_anonima, callback_function, nombre_cola)       
 
    def subscribirse_cola(self, nombre_cola, callback_function):
       resultado = self._channel.queue_declare(queue='')
       nombre_cola_anonima = resultado.method.queue
       self._channel.queue_bind(exchange=nombre_cola, queue=nombre_cola_anonima)           
       self.vincular_wrapper(nombre_cola_anonima, callback_function, nombre_cola) 

       
    def dejar_de_consumir(self, nombre_cola):
        consumer_tag = self._consumer_tags[nombre_cola]
        self._channel.basic_cancel(consumer_tag) 
        del self._wrapers[nombre_cola]

    def crear_cola_por_topico(self, nombre_cola):
        self._channel.exchange_declare(exchange=nombre_cola, exchange_type='direct')
        

    def consumir(self):
       self._channel.start_consuming()

    def enviar_mensaje_por_topico(self, nombre_cola, mensaje, topico):
        self._channel.basic_publish(exchange=nombre_cola, routing_key=str(topico), body=mensaje)


    def crear_cola_subscriptores(self, nombre_cola):
        self._channel.exchange_declare(exchange=nombre_cola, exchange_type='fanout')

    def enviar_mensaje(self, nombre_cola, mensaje):
        self._channel.basic_publish(exchange='', routing_key=nombre_cola, body=mensaje)

    def cerrar(self):
        self._channel.close()

