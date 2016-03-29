#!/bin/bash

#####################################
# please customize the below fields #

s3_bucket=''
s3_folder=''
binary_path=/path/to/binary/files

#####################################

binary_files=initialized

send_files(){
#check for flow binaries
binary_files=(`ls $binary_path | grep -v .current`)

for file in ${binary_files[@]};
do
	echo "moving $file..."
	aws s3 mv $binary_path/$file s3://$s3_bucket/$s3_folder/$file --exclude "nfcapd.current"
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
	sleep 300
done