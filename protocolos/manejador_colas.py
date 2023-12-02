#!/usr/bin/env python3
import logging
from math import log
import queue
import pika
import os


HOST = 'rabbitmq'

class Wrapper:
    def __init__(self, callback_function, auto_ack=True, post_ack_callback=None):
        self.callback_function = callback_function
        self._post_ack_callback = post_ack_callback
        self._auto_ack = auto_ack        

    def funcion_wrapper(self, channel, method, properties, body):
        hacer_ack = False
        if self._auto_ack:
            self.callback_function(body)
            hacer_ack = True
        # Si no hay auto_ack, se espera que la callback_funcion devuelva un bool que indique si hacer el ACK o no
        else:
            hacer_ack = self.callback_function(body)
            
        if hacer_ack:
            channel.basic_ack(delivery_tag=method.delivery_tag)
            if self._post_ack_callback:
                self._post_ack_callback(body)
        else:
            channel.basic_nack(delivery_tag=method.delivery_tag)

            

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
        self._channel.queue_declare(queue=nombre_cola, durable=True)
    
    def crear_cola_por_topico(self, nombre_cola):
        self._channel.exchange_declare(exchange=nombre_cola, exchange_type='direct', durable=True)
        
    def crear_cola_subscriptores(self, nombre_cola):
        self._channel.exchange_declare(exchange=nombre_cola, exchange_type='fanout', durable=True)
        

    def vincular_wrapper(self, nombre_cola, callback_function, auto_ack=True, post_ack_callback=None):
       nombre_queue=nombre_cola
       if (nombre_cola in self._nombrecolas):
            nombre_queue=self._nombrecolas[nombre_cola]
            
       wr = Wrapper(callback_function, auto_ack, post_ack_callback)
       self._consumer_tags[nombre_cola] = self._channel.basic_consume(queue=nombre_queue, 
        on_message_callback=wr.funcion_wrapper)
       self._wrapers[nombre_cola] = wr
        

    def consumir_mensajes(self, nombre_cola, callback_function, auto_ack=True):
       self.vincular_wrapper(nombre_cola, callback_function, auto_ack)

    def consumir_mensajes_por_topico(self, nombre_cola, callback_function, topico, auto_ack=True):
       resultado = self._channel.queue_declare(queue='', durable=True)
       nombre_cola_anonima = resultado.method.queue
       self._nombrecolas[nombre_cola] = nombre_cola_anonima
       self._channel.queue_bind(exchange=nombre_cola, queue=nombre_cola_anonima, routing_key=str(topico))       
       self.vincular_wrapper(nombre_cola, callback_function, auto_ack)       
 
    def subscribirse_cola(self, nombre_cola, callback_function, auto_ack=True):
       resultado = self._channel.queue_declare(queue='', durable=True)
       nombre_cola_anonima = resultado.method.queue
       self._nombrecolas[nombre_cola] = nombre_cola_anonima
       self._channel.queue_bind(exchange=nombre_cola, queue=nombre_cola_anonima)           
       self.vincular_wrapper(nombre_cola, callback_function, auto_ack) 

    def dejar_de_consumir(self, nombre_cola):
        try:
            consumer_tag = self._consumer_tags[nombre_cola]
            self._channel.basic_cancel(consumer_tag)
            #del self._wrapers[nombre_cola]
        except KeyError:
            return

    def consumir(self):
        self._channel.start_consuming()


    def enviar_mensaje_por_topico(self, nombre_cola, mensaje, topico):
        self._channel.basic_publish(exchange=nombre_cola, routing_key=str(topico), body=mensaje, properties=pika.BasicProperties(
            delivery_mode=pika.DeliveryMode.Transient.Persistent
        ))

        
    def enviar_mensaje_suscriptores(self, nombre_cola, mensaje):
        logging.debug(f"Enviando mensaje suscriptores al exchange={nombre_cola} mensaje={mensaje}")
        self._channel.basic_publish(exchange=nombre_cola, routing_key='', body=mensaje, properties=pika.BasicProperties(
            delivery_mode=pika.DeliveryMode.Transient.Persistent
        ))

    def enviar_mensaje(self, nombre_cola, mensaje):
        logging.debug(f"Enviando mensaje al routing_key={nombre_cola} mensaje={mensaje}")
        self._channel.basic_publish(exchange='', routing_key=nombre_cola, body=mensaje, properties=pika.BasicProperties(
            delivery_mode=pika.DeliveryMode.Transient.Persistent
        ))
        

    def cerrar(self):
        try:
            self._channel.close()
        except:
            pass