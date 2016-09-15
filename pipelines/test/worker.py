#!/bin/env python

import subprocess
import datetime
import logging
import os
import sys
import json 
from multiprocessing import Process
from oni.utils import Util


class Worker(object):

    def __init__(self,db_name,hdfs_app_path,kafka_consumer):
        self._initialize_members(db_name,hdfs_app_path,kafka_consumer)

    def _initialize_members(self,db_name,hdfs_app_path,kafka_consumer):

        # get logger instance.
        self._logger = Util.get_logger('ONI.INGEST.WRK.TEST')

        self._db_name = db_name
        self._hdfs_app_path = hdfs_app_path

        # read proxy configuration.
        self._script_path = os.path.dirname(os.path.abspath(__file__))
        conf_file = "{0}/test_conf.json".format(self._script_path)
        self._conf = json.loads(open(conf_file).read())

        self._process_opt = self._conf['process_opt']
        self._local_staging = self._conf['local_staging']
        self.kafka_consumer = kafka_consumer

    def start(self):

        try:
            self._logger.info("Listening topic:{0}".format(self.kafka_consumer.Topic))
            for msg in [self.kafka_consumer.start()]:
                self._logger.info('{0} at offset {1} with key {2}: {3}'.format(msg.topic(), msg.partition(), msg.offset(), str(msg.key())))
                self._new_file(msg.value())

        except KeyboardInterrupt:
            self._logger.info('exiting')
            raise SystemExit
        except:
            print "Unexpected error:", sys.exc_info()[0]
            raise
        finally:
            self.kafka_consumer.stop()

    def _new_file(self,file):

        self._logger.info("-------------------------------------- New File received --------------------------------------")
        self._logger.info("File: {0} ".format(file))        
        p = Process(target=self._process_new_file, args=(file,))
        p.start()        

    def _process_new_file(self,file):

        # get file from hdfs
        get_file_cmd = "hadoop fs -get {0} {1}.".format(file,self._local_staging)
        self._logger.info("Getting file from hdfs: {0}".format(get_file_cmd))
        #Util.execute_cmd(get_file_cmd,self._logger)
        print(get_file_cmd)

        # get file name and date
        file_name_parts = file.split('/')
        file_name = file_name_parts[len(file_name_parts)-1]

        flow_date = file_name.split('.')[1]
        flow_year = flow_date[0:4]
        flow_month = flow_date[4:6]
        flow_day = flow_date[6:8]
        flow_hour = flow_date[8:10]

        # build process cmd.
        process_cmd = "nfdump -o csv -r {0}{1} {2} > {0}{1}.csv".format(self._local_staging,file_name,self._process_opt)
        self._logger.info("Processing file: {0}".format(process_cmd))
        #Util.execute_cmd(process_cmd,self._logger)        
        print(process_cmd)

        # create hdfs staging.
        hdfs_path = "{0}/flow".format(self._hdfs_app_path)
        staging_timestamp = datetime.datetime.now().strftime('%M%S%f')[:-4]
        hdfs_staging_path =  "{0}/stage/{1}".format(hdfs_path,staging_timestamp)
        create_staging_cmd = "hadoop fs -mkdir -p {0}".format(hdfs_staging_path)
        self._logger.info("Creating staging: {0}".format(create_staging_cmd))
        #Util.execute_cmd(create_staging_cmd,self._logger)
        print(create_staging_cmd)
        
        # move to stage.
        mv_to_staging ="hadoop fs -moveFromLocal {0}{1}.csv {2}/.".format(self._local_staging,file_name,hdfs_staging_path)
        self._logger.info("Moving data to staging: {0}".format(mv_to_staging))
        #subprocess.call(mv_to_staging,shell=True)
        print(mv_to_staging)

        #load to avro
        load_to_avro_cmd = "hive -hiveconf dbname={0} -hiveconf y={1} -hiveconf m={2} -hiveconf d={3} -hiveconf h={4} -hiveconf data_location='{5}' -f pipelines/flow/load_flow_avro_parquet.hql".format(self._db_name,flow_year,flow_month,flow_day,flow_hour,hdfs_staging_path)
        self._logger.info( "Loading data to hive: {0}".format(load_to_avro_cmd))
        #Util.execute_cmd(load_to_avro_cmd,self._logger)
        print(load_to_avro_cmd)

        # remove from hdfs staging
        rm_hdfs_staging_cmd = "hadoop fs -rm -R -skipTrash {0}".format(hdfs_staging_path)
        self._logger.info("Removing staging path: {0}".format(rm_hdfs_staging_cmd))
        #Util.execute_cmd(rm_hdfs_staging_cmd,self._logger)
        print(rm_hdfs_staging_cmd)

        # remove from local staging.
        rm_local_staging = "rm {0}{1}".format(self._local_staging,file_name)
        self._logger.info("Removing files from local staging: {0}".format(rm_local_staging))
        #Util.execute_cmd(rm_local_staging,self._logger)
        print(rm_local_staging
        )
        self._logger.info("File {0} was successfully processed.".format(file_name))


