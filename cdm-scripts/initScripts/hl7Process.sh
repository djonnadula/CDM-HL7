#!/bin/bash
# /etc/init.d/hl7process
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
. /etc/rc.d/init.d/functions

#
# Settings
#

# Process name ( For display )
NAME='hl7Process'
# Daemon name, where is the actual executable
SERVICEDIR='/hadoop/cdm/lib'

SERVICE='hl7process.jar'

# Java settings
MINHEAP=512
MAXHEAP=512
JMXPORT=50000
OPTIONS='/hadoop/cdm/cfg/CDMHL7.properties'
INVOCATION="java -Xms${MINHEAP}M -Xmx${MAXHEAP}M \
-cp hl7process.jar com.hca.cdm.job.Hl7Driver ${OPTIONS} \
"

# pid file for the daemon
PIDFILE=/var/log/cdm/cdm-hl7/run/$NAME.pid
LOCKFILE=/var/log/cdm/cdm-hl7/lock/subsys/$NAME

# Exit if the package is not installed
if [ ! -e "$SERVICEDIR/$SERVICE" ]; then
  echo "Couldn't find $NAME"
  exit 1;
fi

# Generate Ticket
TICKET="kinit -k -t/home/corpsrvcdmbtch/corpsrvcdmbtch.keytab corpsrvcdmbtch@HCA.CORPAD.NET"
$TICKET
VALIDTICKET=$?
if [[ ${VALIDTICKET} > 0 ]]; then
  echo "Generating Kerberos Ticket Failed, exiting .. $NAME"
    exit 1;
   	fi

RETVAL=0

start() {
    if [ -a $LOCKFILE ];
        then
                echo "$NAME appears to be running, or has crashed, or was not stopped properly."
                echo "check $PIDFILE, and remove $LOCKFILE to start again."
                return -1;
    fi
    echo -n $"Starting $NAME: "
    set +e
    cd ${SERVICEDIR} && \
    nohup $INVOCATION >>"/var/log/cdm/cdm-hl7/logs/logfile" 2>>"/var/log/cdm/cdm-hl7/logs/errorlog" &
    pgrep -f $SERVICE > $PIDFILE
    RETVAL=$?
    echo
    [ $RETVAL = 0 ] && touch ${LOCKFILE}
    return $RETVAL
}

stop() {
    echo -n $"Stopping $NAME: "
    killproc -p ${PIDFILE} ${NAME}
    RETVAL=$?
    echo
    [ $RETVAL = 0 ] && rm -f ${LOCKFILE} ${PIDFILE}
}

rh_status() {
    status -p ${PIDFILE} ${INVOCATION}
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
    status)
        rh_status
        RETVAL=$?
        ;;
    *)
        echo $"Usage: $NAME start|stop|status"
        RETVAL=1
esac

exit $RETVAL;