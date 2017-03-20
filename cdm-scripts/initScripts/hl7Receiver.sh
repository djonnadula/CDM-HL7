#!/bin/bash
# /etc/init.d/hl7Receiver
# version 1.0.0 2016-08-22 (YYYY-MM-DD)
#
# chkconfig: - 86 06
# description:  Starts and stops HCA CDM HL7 Receiver service
#
### BEGIN INIT INFO
# Provides:          hl7Receiver
# Required-Stop:     $network
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Description:      This service consumes Streams for different HL7 Message Types From Upstream BIZTALK and Lands data on Kafka


### END INIT INFO

# Source function library.
. /etc/rc.d/init.d/functions

#
# Settings
#

# Process name ( For display )
NAME='CDM-HL7-Receiver'
# Daemon name, where is the actual executable
SERVICEDIR='/hadoop/cdm/lib'

SERVICE='hl7process.jar'

# Java settings
MINHEAP=512
MAXHEAP=512
JMXPORT=50000
CONFIG='/hadoop/cdm/cfg/Receiver.properties'
INVOCATION="java -Xms${MINHEAP}M -Xmx${MAXHEAP}M \
-cp hl7process.jar com.hca.cdm.job.Hl7Driver ${CONFIG} \
"

# pid file for the daemon
PIDFILE=/var/log/cdm/cdm-hl7-reveiver/run/$NAME.pid
LOCKFILE=/var/log/cdm/cdm-hl7-reveiver/lock/subsys/$NAME
OUTFILE=/var/log/cdm/cdm-hl7-reveiver/logs/logfile
ERRORFILE=/var/log/cdm/cdm-hl7-reveiver/logs/errorlog

# Exit if the package is not installed
if [ ! -e "$SERVICEDIR/$SERVICE" ]; then
  echo "Couldn't find $NAME"
  exit 1;
fi

RETVAL=0

start() {
    if [ -e $LOCKFILE ];
        then
                echo "$NAME appears to be running, or has crashed, or was not stopped properly."
                echo "check $PIDFILE, and remove $LOCKFILE to start again."
                return 1;
    fi
    echo -n $"Starting $NAME: "
    set +e
    cd ${SERVICEDIR} && \
    nohup $INVOCATION >> $OUTFILE 2>> $ERRORFILE &
    pgrep -f $CONFIG > $PIDFILE
    RETVAL=$?
    echo
    [ $RETVAL = 0 ] && touch ${LOCKFILE}
}

stop() {
    echo -n $"Stopping $NAME: "
    killproc -p ${PIDFILE}${NAME}
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
        rh_status >/dev/null 2>&1 && exit 0;
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

exit $RETVAL;