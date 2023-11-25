#!/usr/bin/env python3
import logging
from math import log
import queue
import pika
import os

HOST = 'rabbitmq'

class Wrapper:
    def __init__(self, callback_function):
        self.callback_function = callback_function        

    def funcion_wrapper(self, channel, method, properties, body):
        nombre_archivo, contenido = self.callback_function(body)
        channel.basic_ack(delivery_tag=method.delivery_tag)

class ManejadorColas:
    def __init__(self):
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=HOST))
        self._channel = connection.channel()
        self._channel.basic_qos(prefetch_count=1)
        self._consumer_tags = {}
        self._wrapers = {}
        self._nombrecolas = {}

    def crear_cola(self, nombre_cola):
        self._channel.queue_declare(queue=nombre_cola)
    
    def crear_cola_por_topico(self, nombre_cola):
        self._channel.exchange_declare(exchange=nombre_cola, exchange_type='direct')
        
    def crear_cola_subscriptores(self, nombre_cola):
        self._channel.exchange_declare(exchange=nombre_cola, exchange_type='fanout')
        

    def vincular_wrapper(self, nombre_cola, callback_function):
       nombre_queue=nombre_cola
       if (nombre_cola in self._nombrecolas):
            nombre_queue=self._nombrecolas[nombre_cola]
            
       wr = Wrapper(callback_function)
       self._consumer_tags[nombre_cola] = self._channel.basic_consume(queue=nombre_queue, 
        on_message_callback=wr.funcion_wrapper)
       self._wrapers[nombre_cola] = wr
        

    def consumir_mensajes(self, nombre_cola, callback_function):
       self.vincular_wrapper(nombre_cola, callback_function)

    def consumir_mensajes_por_topico(self, nombre_cola, callback_function, topico):
       resultado = self._channel.queue_declare(queue='')
       nombre_cola_anonima = resultado.method.queue
       self._nombrecolas[nombre_cola] = nombre_cola_anonima
       self._channel.queue_bind(exchange=nombre_cola, queue=nombre_cola_anonima, routing_key=str(topico))       
       self.vincular_wrapper(nombre_cola, callback_function)       
 
    def subscribirse_cola(self, nombre_cola, callback_function):
       resultado = self._channel.queue_declare(queue='')
       nombre_cola_anonima = resultado.method.queue
       self._nombrecolas[nombre_cola] = nombre_cola_anonima
       self._channel.queue_bind(exchange=nombre_cola, queue=nombre_cola_anonima)           
       self.vincular_wrapper(nombre_cola, callback_function) 

    def dejar_de_consumir(self, nombre_cola):
        try:
            consumer_tag = self._consumer_tags[nombre_cola]
            self._channel.basic_cancel(consumer_tag)
            #del self._wrapers[nombre_cola]
        except KeyError:
            return

    def consumir(self):
        try:
           self._channel.start_consuming()
        except (pika.exceptions.ConnectionClosedByBroker, pika.exceptions.StreamLostError, pika.exceptions.ChannelWrongStateError):
            logging.error('Conexión perdida')

    def enviar_mensaje_por_topico(self, nombre_cola, mensaje, topico):
        self._channel.basic_publish(exchange=nombre_cola, routing_key=str(topico), body=mensaje)

        
    def enviar_mensaje_suscriptores(self, nombre_cola, mensaje):
        logging.debug(f"Enviando mensaje suscriptores al exchange={nombre_cola} mensaje={mensaje}")
        self._channel.basic_publish(exchange=nombre_cola, routing_key='', body=mensaje)

    def enviar_mensaje(self, nombre_cola, mensaje):
        try:
            logging.debug(f"Enviando mensaje al routing_key={nombre_cola} mensaje={mensaje}")
            self._channel.basic_publish(exchange='', routing_key=nombre_cola, body=mensaje)
        except (pika.exceptions.ConnectionClosedByBroker, pika.exceptions.StreamLostError, pika.exceptions.ChannelWrongStateError):
            logging.error('Error al enviar mensaje, se cerró la conexión')

    def cerrar(self):
        try:
            self._channel.close()
        except:
            pass



    def recuperar_siguiente_checkpoint(self):
        mylist = range(0)
        for i in mylist:
            yield i