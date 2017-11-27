#!/bin/bash
# /etc/init.d/hl7CdmAvis
# version 1.0.0 2016-08-22 (YYYY-MM-DD)
#
# chkconfig: - 86 06
# description:  Starts and stops HCA CDM HL7 AVIS EDW Service
#
### BEGIN INIT INFO
# Provides:          hl7CdmAvis
# Required-Stop:     $network
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: Bridge for Loading Staging area in Teradata

### END INIT INFO

# Source function library.
. /etc/init.d/functions

#
# Settings
#

# Process name ( For display )
NAME='CDM-HL7-AVIS-EDW'
# Daemon name, where is the actual executable
SERVICEDIR='/hadoop/cdm/avis/edw/latest/lib'

SERVICE='hl7process.jar'

# Java settings
MINHEAP=512
MAXHEAP=512
JMXPORT=50000
CONFIG='/hadoop/cdm/avis/edw/latest/AvisEdh.properties'
INVOCATION="java -Xms${MINHEAP}M -Xmx${MAXHEAP}M \
-cp  ${SERVICE}:/opt/cloudera/parcels/CDH/jars/*  com.hca.cdm.job.avis.AvisEdhJob ${CONFIG} \
"

# pid file for the daemon
OUTFILE=/var/log/cdm/avis/edw/logs/logfile
ERRORFILE=/var/log/cdm/avis/edw/logs/errorlog

# Exit if the package is not installed
if [ ! -e "$SERVICEDIR/$SERVICE" ]; then
  echo "Couldn't find $NAME"
  exit 1;
fi


KeyTab_Check()
{
# Generate Ticket
TICKET="timeout 60 kinit -k -t /home/corpsrvcdmbtch/corpsrvcdmbtch.keytab corpsrvcdmbtch@HCA.CORPAD.NET"
echo "Checking Valid Kerberos Key Ticket ..."

timeout 60 klist | grep HCA.CORPAD.NET

ticketExists=$?
if [[ "$ticketExists" -eq 0 ]];then
	echo "You got a ticket, So no kinit stuff...";
else
	echo "No ticket, So kinit'ing...";
        $TICKET
        VALIDTICKET=$?

        if [[ ${VALIDTICKET} > 0 ]]; then
           echo "Generating Kerberos Ticket Failed, exiting verification for .. $NAME"
           exit 1;
        fi
        echo "Generating Kerberos Ticket has completed"
fi
}

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

KeyTab_Check
start
