#!/bin/env python

from multiprocessing import Process
import time
import os
import boto3 as boto
import botocore
from oni.utils import Util



class flow_ingest(object):



	def __init__(self,conf):

            self._initialize_members(conf)

    def _initialize_members(self,conf):

            self._collector_path = None
            self._hdfs_root_path = None
            self._queue_name = None
            self._dsource = 'flow'
            
            # validate configuration info.
            conf_err_msg = "Please provide a valid '{0}' in the configuration file"
            Util.validate_parameter(conf['collector_path'],conf_err_msg.format("collector_path"))
            Util.validate_parameter(conf['queue_name'],conf_err_msg.format("queue_name"))
            # set configuration.
            self._collector_path = conf['collector_path']
            self._hdfs_root_path = "{0}/{1}".format(conf['huser'] , self._dsource)
            self._queue_name = conf['queue_name']


        def start(self):

            try:
                while True:
                    time.sleep(5)
                    print "Watching the S3 Bucket: {0} to collect files".format(s3_bucket)
                    file_list=client.list_objects(Bucket=s3_bucket,Prefix=staging_folder)['Contents']
                    for key in file_list:
                        file_name_parts = key['Key'].split('/')
                        file_name = file_name_parts[len(file_name_parts)-1]
                        filename_str = str(file_name)
                        # get file name and date
                        if filename_str and (not filename_str.isspace()):
                            binary_year,binary_month,binary_day,binary_hour,binary_date_path =  Util.build_hdfs_path(file_name,self._dsource)
                            # hdfs path with timestamp.
                            aws_archive_path = "{0}/binary/{1}/{2}".format(archive_folder,binary_date_path,binary_hour)
                        if filename_str and (not filename_str.isspace()):
                            if not  ".current" in file_name:
                                print file_name
                                # get file from AWS_s3
                                client.download_file(s3_bucket, '{0}/{1}'.format(staging_folder,file_name), '../stage/{0}'.format(file_name))
                                p = Process(target=Util.send_new_file_notification, args=(file_name,self._queue_name))
                                p.start()
                                p.join()
                                # move processed binary file to archive for holding
                                s3.Object(s3_bucket,'{0}/{1}'.format(aws_archive_path,file_name)).copy_from(CopySource='{0}/{1}/{2}'.format(s3_bucket,staging_folder,file_name))
                                #delete staging file in s3
                                s3.Object(s3_bucket,'{0}/{1}'.format(staging_folder,file_name)).delete()
                                print "Done !!!!!"

            except KeyboardInterrupt:
                print "exiting"


