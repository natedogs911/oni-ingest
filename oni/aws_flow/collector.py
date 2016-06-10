#!/bin/env python

import time
import boto3 as boto
import botocore
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from multiprocessing import Process
from oni.utils import Util

client = boto.client('s3')
s3 = boto.resource('s3')

class Collector(object):

    def __init__(self,conf,app_path,mb_producer):
        self._initialize_members(conf,app_path,mb_producer)

    def _initialize_members(self,conf,app_path,mb_producer):


        # validate configuration info.
        conf_err_msg = "Please provide a valid '{0}' in the configuration file"
        Util.validate_parameter(conf['collector_path'],conf_err_msg.format("collector_path"))
        Util.validate_parameter(conf['topic'],conf_err_msg.format("topic"))
        Util.validate_parameter(app_path,conf_err_msg.format("huser"))

        # set configuration.
        self._collector_path = conf['collector_path']
        self._dsource = 'flow'
        self._hdfs_root_path = "{0}/{1}".format(app_path, self._dsource)
        self._topic = conf['topic']

        # initialize message broker client.
        self._mb_producer = mb_producer

        # aws s3 config

        self._s3_bucket = conf['s3_bucket']
        self._staging_folder = conf['staging_folder']
        self._archive_folder = conf['archive_folder']
        bucket_check = validate_bucket(self._s3_bucket)
        if bucket_check == "false":
            print "AWS bucket does not exist"
            print "Please check AWS S3 configuration"
            quit()

    def start(self):

        # start watchdog
        print "Watching the S3 Bucket: {0} to collect files".format(self._s3_bucket)

        try:
            while True:
                time.sleep(60)
                binary_file = aws_file(self._s3_bucket, self._staging_folder)
                load_new_file(binary_file)

        except KeyboardInterrupt:

    def load_new_file(self,file):

        # create new process for the new file.
        print "---------------------------------------------------------------------------"
        print "New File received: {0}".format(file)
        if not  ".current" in file:
            p = Process(target=self._ingest_file, args=(file,self._mb_producer.Partition))
            p.start()
            p.join()

    def _ingest_file(self,file,partition):

        # get file name and date.
        file_name = file
        file_date = file.split('.')[1]
        file_date_path = file_date[0:8]
        file_date_hour = file_date[8:10]

        #aws archive path
        aws_archive_path = "{0}/binary/{1}/{2}".format(self._archive_folder,file_date_path,file_date_hour)
        aws_load_to_archive(self._s3_bucket,self._staging_folder,aws_archive_path,file)

        #download file from s3 to stage folder
        aws_download_file(self._s3_bucket, self._staging_folder, '../stage/{0}'.format(file_name))

        # hdfs path with timestamp.
        hdfs_path = "{0}/binary/{1}/{2}".format(self._hdfs_root_path,file_date_path,file_date_hour)
        Util.creat_hdfs_folder(hdfs_path)

        # load to hdfs.
        hdfs_file = "{0}/{1}".format(hdfs_path,file_name)
        Util.load_to_hdfs('../stage/{0}'.format(file_name),hdfs_file)

        # create event for workers to process the file.
        print "Sending file to worker number: {0}".format(partition)
        self._mb_producer.create_message(hdfs_file,partition)

        print "File has been successfully moved to: {0}".format(partition)

    def validate_bucket(bucket):
        check = {}
        bucket_list = client.list_buckets()['Buckets']
        for line in bucket_list:
            if line['Name'] == bucket:
                check = 'true'

        if check == 'true':
            return "true"
        else:
            return "false"

    def aws_file(self,bucket,binary_staging):

        #find the oldest file in the s3 bucket
        s3_complete_list = client.list_objects(Bucket=bucket, Prefix=binary_staging)['Contents']
        s3_sorted_list = sorted(s3_complete_list, key=lambda l: ['Key'])
        key = s3_sorted_list[0]['Key']
        file_name_parts = key['Key'].split('/')
        file = file_name_parts[len(file_name_parts) - 1]

        return file

        except KeyError:
            print "This bucket either does not exist or is empty"
                    #todo get archive path
                    archive_path = "{0}/binary/{1}/{2}".format(self._s3, file_date_path, file_date_hour)

    def aws_load_to_archive(bucket,path,destination,file):

        # move processed binary file to archive for holding
        s3.Object(bucket, '{0}/{1}'.format(destination, file)).copy_from(CopySource='{0}/{1}/{2}'.format(bucket, path, file))

    def aws_download_file(bucket,path,destination,file)

        client.download_file(bucket, '{0}/{1}'.format(path,file), destination)
        # delete staging file in s3
        s3.Object(bucket, '{0}/{1}'.format(path, file)).delete()