#!/usr/bin/env bash
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
NAME='hl7process'
# Daemon name, where is the actual executable
SERVICEDIR='/home/hadoop/cdm/hl7/prod'

SERVICE='hl7process.jar'

# Java settings
MINHEAP=5120
MAXHEAP=8192
JMXPORT=50000
OPTIONS='CDMHL7.properties'
INVOCATION="java  \
-cp hl7process.jar com.hca.cdm.job.HL7Job ${OPTIONS} \
"

# pid file for the daemon
PIDFILE=${SERVICEDIR}/var/run/$NAME.pid
LOCKFILE=${SERVICEDIR}/var/lock/subsys/$NAME

# Exit if the package is not installed
if [ ! -e "$SERVICEDIR/$SERVICE" ]; then
{
  echo "Couldn't find $NAME"
  exit 99
}
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
    nohup $INVOCATION >>"logfile" 2>"errorlog" &

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

reload() {
    echo -n $"Reloading $NAME: "
    killproc -p ${PIDFILE} ${NAME} -HUP
    RETVAL=$?
    echo
}

upgrade() {
    oldbinpidfile=${PIDFILE}.oldbin

    configtest -q || return 6
    echo -n $"Staring new master $NAME: "
    killproc -p ${PIDFILE} ${NAME} -USR2
    RETVAL=$?
    echo
    /bin/usleep $SLEEPMSEC
    if [ -f ${oldbinpidfile} -a -f ${PIDFILE} ]; then
        echo -n $"Graceful shutdown of old $NAME: "
        killproc -p ${oldbinpidfile} ${NAME} -QUIT
        RETVAL=$?
        echo
    else
        echo $"Upgrade failed!"
        return 1
    fi
}

configtest() {
    if [ "$#" -ne 0 ] ; then
        case "$1" in
            -q)
                FLAG=$1
                ;;
            *)
                ;;
        esac
        shift
    fi
    ${INVOCATION} -t -c ${conffile} $FLAG
    RETVAL=$?
    return $RETVAL
}

rh_status() {
    status -p ${PIDFILE} ${INVOCATION}
}

# See how we were called.
case "$1" in
    start)
        rh_status >/dev/null 2>&1 && exit 0
        start
        ;;
    stop)
        stop
        ;;
    status)
        rh_status
        RETVAL=$?
        ;;
    restart)
        configtest -q || exit $RETVAL
        stop
        start
        ;;
    upgrade)
        upgrade
        ;;
    condrestart|try-restart)
        if rh_status >/dev/null 2>&1; then
            stop
            start
        fi
        ;;
    force-reload|reload)
        reload
        ;;
    configtest)
        configtest
        ;;
    *)
        echo $"Usage: $NAME {start|stop|restart|condrestart|try-restart|force-reload|upgrade|reload|status|help|configtest}"
        RETVAL=2
esac

exit $RETVAL