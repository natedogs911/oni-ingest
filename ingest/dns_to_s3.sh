#!/bin/bash

#####################################
# please customize the below fields #

s3_bucket='bucketname'
s3_folder='bucket_subfolder'
binary_path=/path/to/local/files

#####################################
initial_list=initialized
binary_files=initialized
exclude_file=initialized

send_files(){


#locating and excluding currently generating .pcap file
initial_list=(`ls $binary_path | sort -g -r`)
excluded_file=${initial_list[0]}
binary_files=(`ls $binary_path | grep -v ${excluded_file}`)

for file in ${binary_files[@]};
do
    echo "moving $file..."
    aws s3 mv $binary_path/$file s3://$s3_bucket/$s3_folder/$file
done
}

control_c(){
  echo "exiting, this will take a moment"
  exit $?
}

trap control_c SIGINT

while :
do
	echo "Press [CTRL+C] to stop.."
	send_files
	sleep 10
done
