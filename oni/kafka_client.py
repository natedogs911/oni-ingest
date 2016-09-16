#!/bin/env python 
import logging
import os
from oni.utils import Util
from kafka import KafkaProducer
from kafka import KafkaConsumer as KC
from kafka.partitioner.roundrobin import RoundRobinPartitioner
from kafka.common import TopicPartition

#librdkafka kerberos configs
krb_conf_options = {'sasl.mechanisms': 'gssapi',
                            'security.protocol': 'sasl_plaintext',
                            'sasl.kerberos.service.name': 'kafka',
                            'sasl.kerberos.kinit.cmd': 'kinit -k -t "%{sasl.kerberos.keytab}" %{sasl.kerberos.principal}',
                            'sasl.kerberos.principal': os.getenv('KRB_USER'),
                            'sasl.kerberos.keytab': os.getenv('KEYTABPATH'),
                            'sasl.kerberos.min.time.before.relogin': 60000}

class KafkaTopic(object):


    def __init__(self,topic,server,port,zk_server,zk_port,partitions):

        self._initialize_members(topic,server,port,zk_server,zk_port,partitions)

    def _initialize_members(self,topic,server,port,zk_server,zk_port,partitions):

        # get logger isinstance
        self._logger = logging.getLogger("ONI.INGEST.KAFKA")

        # kafka requirements
        self._server = server
        self._port = port
        self._zk_server = zk_server
        self._zk_port = zk_port
        self._topic = topic
        self._num_of_partitions = partitions
        self._partitions = []
        self._partitioner = None
        self._librdkafka_debug = {'debug': 'all'}

        # create topic with partitions
        self._create_topic()

    def _create_topic(self):

        self._logger.info("Creating topic: {0} with {1} parititions".format(self._topic,self._num_of_partitions))     

        # Create partitions for the workers.
        #self._partitions = [ TopicPartition(self._topic,p) for p in range(int(self._num_of_partitions))]        

        # create partitioner
        #self._partitioner = RoundRobinPartitioner(self._partitions)
        
        # get script path 
        zk_conf = "{0}:{1}".format(self._zk_server,self._zk_port)
        create_topic_cmd = "{0}/kafka_topic.sh create {1} {2} {3}".format(os.path.dirname(os.path.abspath(__file__)),self._topic,zk_conf,self._num_of_partitions)

        # execute create topic cmd
        Util.execute_cmd(create_topic_cmd,self._logger)

    def send_message(self,message,topic_partition):

        self._logger.info("Sending message to: Topic: {0} Partition:{1}".format(self._topic,topic_partition))
        kafka_brokers = '{0}:{1}'.format(self._server,self._port)             
        self._producer_conf = {'bootstrap.servers': kafka_brokers,
                               'session.timeout.ms': 6000, 
                               'api.version.request': 'false',
                               'internal.termination.signal': 0,
                               'broker.version.fallback': '0.9.0.0',
                               'log.connection.close': 'false',
                               'socket.keepalive.enable': 'false', 
                               'default.topic.config': {'request.required.acks': 'all'}}
        
        if os.getenv('ingest_kafka_debug'):
            self._logger.info("librdkafka debug: all")
            self._producer_conf.update(self._librdkafka_debug)
 
        if os.getenv('KRB_AUTH'):
            self._logger.info("Updating Consumer Configuration with Kerberos options")
            self._producer_conf.update(krb_conf_options)
        
        def delivery_callback (err, msg):
            if err:
                self._logger.info('Message failed delivery: {0}'.format(err))
            else:
                self._logger.info('Message delivered to topic {0} on {1}'.format(msg.topic(), msg.partition()))

        producer = confluent_kafka_Producer(**self._producer_conf)
        future = producer.produce(self._topic, message.encode('utf-8'), callback=delivery_callback)
        producer.poll(0)
        producer.flush()

    @property
    def Topic(self):
        return self._topic
    
    @property
    def Partition(self):        
        #return self._partitioner.partition(self._topic).partition
        return 0


class KafkaConsumer(object):
    
    def __init__(self,topic,server,port,zk_server,zk_port,partition):

        self._initialize_members(topic,server,port,zk_server,zk_port,partition)

    def _initialize_members(self,topic,server,port,zk_server,zk_port,partition):

        self._logger = Util.get_logger("ONI.INGEST.KAFKA")
        self._topic = topic
        self._server = server
        self._port = port
        self._zk_server = zk_server
        self._zk_port = zk_port
        self._id = partition
        self._librdkafka_debug = {'debug': 'all'}

    def start(self):
        
        kafka_brokers = '{0}:{1}'.format(self._server,self._port)
        consumer =  KC(bootstrap_servers=[kafka_brokers],group_id=self._topic)
        partition = [TopicPartition(self._topic,int(self._id))]
        consumer.assign(partitions=partition)
        consumer.poll()
        return consumer

    @property
    def Topic(self):
        return self._topic

    @property
    def ZookeperServer(self):
        return "{0}:{1}".format(self._zk_server,self._zk_port)

    
