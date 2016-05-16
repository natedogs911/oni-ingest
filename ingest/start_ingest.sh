#!/bin/bash
source /etc/duxbay.conf
export DBNAME
export HUSER
export KRB_AUTH
export KINITPATH
export KINITOPTS
export KEYTABPATH
export KRB_USER
#-----------------------------------------------------------------------------------
# Validate parameters.
#-----------------------------------------------------------------------------------

INGEST_TYPE=$1
WORKER_NUM=${2:-0}

if [ -z $INGEST_TYPE  ]; then

    echo "Please provide an ingest type (e.g. flow|dns)"
    exit 1

fi

if [ $INGEST_TYPE != "dns" ] && [ $INGEST_TYPE != "flow"  ]; then
    
    echo "Please provide a valid ingest type: flow|dns"
    exit 1

fi

#-----------------------------------------------------------------------------------
# Create screens for Master and Worker.
#-----------------------------------------------------------------------------------

INGEST_DATE=`date +"%H_%M_%S"`

screen -d -m -S OniIngest_${INGEST_TYPE}_${INGEST_DATE}  -s /bin/bash
screen -dr  OniIngest_${INGEST_TYPE}_${INGEST_DATE} -X screen -t Master sh -c "python master.py -t ${INGEST_TYPE}; echo 'Closing Master...'; sleep 60"

#if [[ $KRB_AUTH == true ]]; then
#screen -dr  OniIngest_${INGEST_TYPE}_${INGEST_DATE} -X screen -t Kerberos sh -c "python ./oni/kerberos_sa.py; echo 'Stopping kerberos authentication...'; sleep 60"
#fi



if [ $WORKER_NUM -gt 0 ]; then
	w=1
	while [  $w -le  $WORKER_NUM  ]; 
	do
		screen -dr OniIngest_${INGEST_TYPE}_${INGEST_DATE}  -X screen -t Worker_$w sh -c "python worker.py -t ${INGEST_TYPE}; echo 'Closing worker...'; sleep 60"
		let w=w+1
	done
fi

#-----------------------------------------------------------------------------------
# show outputs.
#-----------------------------------------------------------------------------------
echo "Background ingest process is running: OniIngest_${INGEST_TYPE}_${INGEST_DATE}"
echo "To rejoin the session use: screen -x OniIngest_${INGEST_TYPE}_${INGEST_DATE}"
echo 'To switch between workers and master use: crtl a + shift "'

