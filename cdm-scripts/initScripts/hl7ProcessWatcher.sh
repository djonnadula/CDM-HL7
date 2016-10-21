#!/usr/bin/env bash
# /etc/init.d/hl7ProcessWatcher
# version 1.0.0 2016-10-20 (YYYY-MM-DD)
#
# chkconfig: - 86 06
# description: Watches hl7process Running On Spark
#
### BEGIN INIT INFO
# Provides:          hl7ProcessWatcher
# Required-Stop:     $network
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: Checks with Yarn resource Manager where hl7process Running or Not
# Description:       Checks with Yarn resource Manager where hl7process Running or Not. Zeeke will trigger this and Appropriate Action will be taken in case of Failures

### END INIT INFO
# Spark Job name
NAME="CDM-HL7-Process"

# Generate Ticket
TICKET="kinit -k -t /home/corpsrvcdmbtch_qa/corpsrvcdmbtch_qa.keytab CorpSRVCDMBtch_QA@HCA.CORPAD.NET"
$TICKET
VALIDTICKET=$?

if [[ ${VALIDTICKET} > 0 ]]; then
  echo "Generating Kerberos Ticket Failed, exiting verification for .. $NAME"
    exit 1;
   	fi
# Acceptable States
if (( $(yarn application -appStates RUNNING -list | grep -w ${NAME} | wc -l) > 0 )); then
  echo "$NAME Running"
  exit 0
fi

if (( $(yarn application -appStates CONNECTED -list | grep -w ${NAME} | wc -l) > 0 )); then
  echo "$NAME CONNECTED"
  exit 0;
fi

if (( $(yarn application -appStates SUBMITTED -list | grep -w ${NAME} | wc -l) > 0 )); then
  echo "$NAME SUBMITTED"
  exit 0;
fi


# Alert States
if (( $(yarn application -appStates FINISHED -list | grep -w ${NAME} | wc -l) > 0 )); then
  echo "$NAME FINISHED"
  exit 1;
fi

if (( $(yarn application -appStates FAILED -list | grep -w ${NAME} | wc -l) > 0 )); then
  echo "$NAME FAILED"
  exit 1;
fi

if (( $(yarn application -appStates KILLED -list | grep -w ${NAME} | wc -l) > 0 )); then
  echo "$NAME KILLED"
  exit 1;
fi






