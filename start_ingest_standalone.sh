#!/bin/bash

source /etc/duxbay.conf

export KRB_AUTH
export KINITPATH
export KINITOPTS
export KEYTABPATH
export KRB_USER

#-----------------------------------------------------------------------------------
# Validate parameters.
#-----------------------------------------------------------------------------------

INGEST_TYPE=$1
WORKERS_NUM=${2}
TIME_ZONE=${3:-UTC}

if [ -z $INGEST_TYPE  ]; then

    echo "Please provide an ingest type (e.g. flow|dns|proxy)"
    exit 1

fi

if [ -z $WORKERS_NUM ];then

    echo "Please provide the number of workers"
    exit 1

fi


if [ $INGEST_TYPE != "dns" ] && [ $INGEST_TYPE != "flow" ]  && [  $INGEST_TYPE != "proxy" ] && [  $INGEST_TYPE != "test" ] ; then
    
    echo "Please provide a valid ingest type: flow|dns|proxy"
    exit 1

fi

#-----------------------------------------------------------------------------------
# Create screens for Master and Worker.
#-----------------------------------------------------------------------------------

INGEST_DATE=`date +"%H_%M_%S"`

screen -d -m -S OniIngest_${INGEST_TYPE}_${INGEST_DATE}  -s /bin/bash
screen -S OniIngest_${INGEST_TYPE}_${INGEST_DATE} -X setenv TZ ${TIME_ZONE}
screen -dr  OniIngest_${INGEST_TYPE}_${INGEST_DATE} -X screen -t Master sh -c "python master_collector.py -t ${INGEST_TYPE} -w ${WORKERS_NUM} -id OniIngest_${INGEST_TYPE}_${INGEST_DATE}; echo 'Closing Master...'; sleep 432000"

echo "Creating master collector"; sleep 3

if [ $WORKERS_NUM -gt 0 ]; then
	w=0
    while [  $w -le  $((WORKERS_NUM-1)) ]; 
	do
        echo "Creating worker_${w}"
		screen -dr OniIngest_${INGEST_TYPE}_${INGEST_DATE}  -X screen -t Worker_$w sh -c "python worker.py -t ${INGEST_TYPE} -i ${w} -top OniIngest_${INGEST_TYPE}_${INGEST_DATE}; echo 'Closing worker...'; sleep 432000"
		let w=w+1
        sleep 2
	done
fi

#-----------------------------------------------------------------------------------
# show outputs.
#-----------------------------------------------------------------------------------
echo "Background ingest process is running: OniIngest_${INGEST_TYPE}_${INGEST_DATE}"
echo "To rejoin the session use: screen -x OniIngest_${INGEST_TYPE}_${INGEST_DATE}"
echo 'To switch between workers and master use: crtl a + "'

