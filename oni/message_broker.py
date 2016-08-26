#!/bin/env python

import os
from confluent_kafka import Producer
from confluent_kafka import Consumer

#build dict based on configs
krb_conf_options = {'sasl.mechanisms': 'gssapi',
        'security.protocol': 'sasl_plaintext',
        'sasl.kerberos.service.name': 'kafka',
        'sasl.kerberos.principal': os.getenv('KRB_USER'),
        'sasl.kerberos.kinit.cmd': os.getenv('KINITOPTS'),
        'sasl.kerberos.keytab': os.getenv('KEYTABPATH'),
        'sasl.kerberos.min.time.before.relogin': 60000}

class Producer(object):

    def __init__(self,server,port,topic,num_partitions):
        
        self._server = server
        self._port = port
        self._topic = topic
        self._partitions = []
        self._partitioner = None
        self._id = id

        #standard conf for kafka
        self._conf = {'bootstrap.servers': self._server,
                      'default.topic.config': {'auto.offset.reset': 'smallest'},
                      'group.id': self._id}
        
        if os.getenv('KRB_AUTH'):
            self._conf.update(krb_conf_options) 
    
    def create_message(self,message,partition=None):
        
        self._producer = Producer(**self._conf)
        future = self._producer.produce(self_._topic, message.encode('utf-8'), partition)
        self._producer.flush()
        self._producer.close()

    @property
    def Server(self):
        return self._server

    @property
    def Port(self):
        return self._port

    @property
    def Partition(self):
        return self._partitioner.partition(self._topic).partition

class Consumer(object):
    
    def __init__(self,server,port,topic,id):
        
        self._server = server
        self._port = port
        self._topic = topic
        self._id = id
        #debug
        print('here')

    #standard conf for kafka
        self._conf = {'bootstrap.servers': self._server,
                      'default.topic.config': {'auto.offset.reset': 'smallest'},
                      'group.id': self._id}
    
        if os.getenv('KRB_AUTH'):
            self._conf.update(krb_conf_options)

    def start(self):
    
        consumer =  Consumer(**self._conf)
        consumer.subscribe(self._topic)
        partition = [TopicPartition(self._topic,int(self._id))]
        consumer.assign(partitions=partition)
        msg = consumer.poll()
        return consumer

    @property
    def Server(self):
        return self._server

    @property
    def Port(self):
        return self._port
