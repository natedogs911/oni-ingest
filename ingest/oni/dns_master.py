#!/bin/env python

import os
import time
import subprocess
import boto3 as boto
import botocore

from oni.utils import Util

s3_bucket = 'bucketname'
staging_folder = 'staging'
archive_folder = 'archive'

client = boto.client('s3')
s3 = boto.resource('s3')

class dns_ingest(object):
    def __init__(self, conf):

        self._initialize_members(conf)

    def _initialize_members(self, conf):

        self._collector_path = None
        self._hdfs_root_path = None
        self._queue_name = None
        self._pkt_num = None
        self._pcap_split_staging = None
        self._time_to_wait = None
        self._dsource = 'dns'

        # valdiate configuration info.
        conf_err_msg = "Please provide a valid '{0}' in the configuration file"
        Util.validate_parameter(conf['collector_path'], conf_err_msg.format("collector_path"))
        Util.validate_parameter(conf['queue_name'], conf_err_msg.format("queue_name"))
        Util.validate_parameter(conf['pkt_num'], conf_err_msg.format("pkt_num"))
        Util.validate_parameter(conf['pcap_split_staging'], conf_err_msg.format("pcap_split_staging"))
        Util.validate_parameter(conf['time_to_wait'], conf_err_msg.format("time_to_wait"))
        #Util.validate_parameter(conf['s3Bucket'], conf_err_msg.format("s3Bucket"))
        #Util.validate_parameter(conf['stagingFolder'], conf_err_msg.format("stagingFolder"))
        #Util.validate_parameter(conf['archiveFolder'], conf_err_msg.format("archiveFolder"))

        # set configuration.
        self._collector_path = conf['collector_path']
        self._hdfs_root_path = "{0}/{1}".format(conf['huser'], self._dsource)
        self._time_to_wait = conf['time_to_wait']
        self._pkt_num = conf['pkt_num']
        self._pcap_split_staging = conf['pcap_split_staging']
        self._queue_name = conf['queue_name']

        #s3Bucket = conf['s3_bucket']
        #stagingFolder = conf['staging_folder']
        #archiveFolder = conf['archive_folder']

    def start(self):

        try:
            while True:
                print "Watching the S3 Bucket: {0} to collect files".format(s3_bucket)
                file_list = client.list_objects(Bucket=s3_bucket, Prefix=staging_folder)['Contents']
                for key in file_list:
                    file_name_parts = key['Key'].split('/')
                    file_name = file_name_parts[len(file_name_parts) - 1]
                    filename_str = str(file_name)
                    # get file name and date
                    if filename_str and (not filename_str.isspace()):
                        print "New file: {0}".format(file_name)
                        binary_year, binary_month, binary_day, binary_hour, binary_date_path = Util.build_hdfs_path(file_name, self._dsource)
                        # local storage path
                        file_local_path = '../stage/{0}'.format(file_name)
                        #aws path with timestamp.
                        aws_archive_path = "{0}/binary/{1}/{2}".format(archive_folder, binary_date_path, binary_hour)
                        # get timestamp from the file name.
                        file_date = file_name.split('.')[0]
                        pcap_hour = file_date[-6:-4]
                        pcap_date_path = file_date[-14:-6]
                        # get file from AWS_s3
                        client.download_file(s3_bucket, '{0}/{1}'.format(staging_folder, file_name), file_local_path)
                        # move binary file to archive for holding
                        s3.Object(s3_bucket, '{0}/{1}'.format(aws_archive_path, file_name)).copy_from(CopySource='{0}/{1}/{2}'.format(s3_bucket, staging_folder, file_name))
                        # delete staging file in s3
                        s3.Object(s3_bucket, '{0}/{1}'.format(staging_folder, file_name)).delete()
                        print "file :{0} staged".format(file_name)
                        if file_name.endswith('.pcap'):
                            self._process_pcap_file(file_name, file_local_path)
                            print "file :{0} processed".format(file_name)
                time.sleep(self._time_to_wait)

        except KeyboardInterrupt:
            print "exiting"

    def _process_pcap_file(self, file_name, file_local_path):

        local_path = os.getenv('LUSER')

        # get file size.
        file_size = os.stat(file_local_path)
        if file_size.st_size > 1145498644:

            # split file.
            self._split_pcap_file(file_name, file_local_path, local_path)
        else:

            # send rabbitmq notification.
            #local_pcap_file = "{0}/{1}".format(local_path, file_name)
            print "Queuing " + file_name
            Util.send_new_file_notification(file_name, self._queue_name)

    def _split_pcap_file(self, file_name, file_local_path, local_path):

        # split file.
        #name = file_name.split('.')[0]
        split_cmd = "editcap -c {0} {1} {2}/{3}_split.pcap".format(self._pkt_num, file_local_path, self._pcap_split_staging, file_name)
        print split_cmd
        subprocess.call(split_cmd, shell=True)

        for currdir, subdir, files in os.walk(self._pcap_split_staging):
            for file in files:
                if file.endswith(".pcap") and "{0}_split".format(file_name) in file:
                    # send rabbitmq notificaion.
                    #local_pcap_file = "{0}/{1}".format(local_path, file)
                    # stage file for processing
                    mv_cmd = "mv {0}/{1} ../stage/".format(self._pcap_split_staging, file)
                    print mv_cmd
                    subprocess.call(mv_cmd, shell=True)
                    print "Queuing " + file
                    Util.send_new_file_notification(file, self._queue_name)

        rm_big_file = "rm {0}".format(file_local_path)
        print rm_big_file
        subprocess.call(rm_big_file, shell=True)
