#!/bin/bash
# /etc/init.d/patidQA
# version 1.0.0 2016-08-22 (YYYY-MM-DD)
#
# chkconfig: - 86 06
# description:  Starts and stops HCA CDM HL7 Processing  service
#
### BEGIN INIT INFO
# Provides:          hl7process
# Required-Stop:     $network
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: Consumes Streams for Different HL7 types from kafka and tarnsform to Json and Pipe messages
# Description:       This service consumes Streams for different HL7 Message Types and  transforms to Json and  applies Data Model for Segments Provided

### END INIT INFO

# Source function library.
. /etc/init.d/functions

#
# Settings
#

# Process name ( For display )
NAME='DataInputToDR'
# Daemon name, where is the actual executable
SERVICEDIR='/hadoop/cdm/patid/latest/lib'

SERVICE='hl7process.jar'

# Java settings
MINHEAP=512
MAXHEAP=512
JMXPORT=50000
CONFIG='/hadoop/cdm/patid/latest/cfg/PatientIdDR.properties'
INVOCATION="java -Xms${MINHEAP}M -Xmx${MAXHEAP}M \
-cp  ${SERVICE}:/opt/cloudera/parcels/CDH/jars/*  com.hca.cdm.job.scri.patid.DataInputToDR ${CONFIG} \
"

# pid file for the daemon
OUTFILE=/var/log/cdm/patid/logs/logfile
ERRORFILE=/var/log/cdm/patid/logs/errorlog

# Exit if the package is not installed
if [ ! -e "$SERVICEDIR/$SERVICE" ]; then
  echo "Couldn't find $NAME"
  exit 1;
fi

RETVAL=0

start() {
    echo -n $"Starting $NAME: "

    cd ${SERVICEDIR} && \
    nohup $INVOCATION >> $OUTFILE 2> $ERRORFILE &
    RETVAL=$?
    echo
    [ $RETVAL -eq 0 ]

    if [[ "$RETVAL" -eq 1 ]];then
      failure "Starting" $NAME

    else
      success "Starting" $NAME

    fi
}
start