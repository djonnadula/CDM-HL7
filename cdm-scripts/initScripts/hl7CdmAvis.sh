#!/bin/bash
# /etc/init.d/hl7CdmAvis
# version 1.0.0 2016-08-22 (YYYY-MM-DD)
#
# chkconfig: - 86 06
# description:  Starts and stops HCA CDM HL7 AVIS Processing  service
#
### BEGIN INIT INFO
# Provides:          hl7CdmAvis
# Required-Stop:     $network
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: Consumes Streams for Vent HL7 types from kafka and tarnsform meaningful way fro down stream consumption

### END INIT INFO

# Source function library.
. /etc/init.d/functions

#
# Settings
#

# Process name ( For display )
NAME='CDM-HL7-AVIS'
# Daemon name, where is the actual executable
SERVICEDIR='/hadoop/cdm/avis/process/latest/lib'

SERVICE='hl7process.jar'

# Java settings
MINHEAP=512
MAXHEAP=512
JMXPORT=50000
CONFIG='/hadoop/cdm/avis/process/latest/AdvancedVent.properties'
INVOCATION="java -Xms${MINHEAP}M -Xmx${MAXHEAP}M \
-cp  ${SERVICE}:/opt/cloudera/parcels/CDH/jars/*  com.hca.cdm.job.Hl7Driver ${CONFIG} \
"

# pid file for the daemon
PIDFILE=/var/log/cdm/avis/process/run/$NAME.pid
LOCKFILE=/var/log/cdm/avis/process/lock/subsys/$NAME
OUTFILE=/var/log/cdm/avis/process/logs/logfile
ERRORFILE=/var/log/cdm/avis/process/logs/errorlog

# Exit if the package is not installed
if [ ! -e "$SERVICEDIR/$SERVICE" ]; then
  echo "Couldn't find $NAME"
  exit 1;
fi

RETVAL=0

start() {
    if [ -e "$LOCKFILE" ];then
                echo "$NAME appears to be running, or has crashed, or was not stopped properly."
                echo "check $PIDFILE, and remove $LOCKFILE to start again."
                exit 1;
    fi
    echo -n $"Starting $NAME: "
    set +e
    cd ${SERVICEDIR} && \
    nohup $INVOCATION >> $OUTFILE 2> $ERRORFILE &
    pgrep -f $CONFIG > $PIDFILE
    RETVAL=$?
    echo
    [ $RETVAL -eq 0 ] && touch ${LOCKFILE}
    if [[ "$RETVAL" -eq 1 ]];then
      failure "Starting" $NAME

    else
      success "Starting" $NAME

    fi
}

stop() {
    echo -n $"Stopping $NAME: "
    killproc -p ${PIDFILE} ${NAME}
    RETVAL=$?
    echo
    [ $RETVAL = 0 ] && rm -f ${LOCKFILE} ${PIDFILE}
}

rh_status() {
    status -p ${PIDFILE} ${NAME}
}

# See how we were called.
case "$1" in
    start)
    rh_status
        start
        ;;
    stop)
        stop
        ;;
    restart)
     	 stop
        start
        ;;
    status)
        rh_status
        RETVAL=$?
        ;;
    *)
        echo $"Usage: $NAME start|stop|status|restart"
        RETVAL=1
esac

if [[ "$RETVAL" -gt 1 ]];then
      exit 1
else
      exit 0
fi