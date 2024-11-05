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

KeyTab_Check()
{
# Generate Ticket
TICKET="timeout 60 kinit -k -t /home/corpsrvcdmbtch/corpsrvcdmbtch.keytab corpsrvcdmbtch@.CORPAD.NET"
echo "Checking Valid Kerberos Key Ticket ..."

timeout 60 klist | grep .CORPAD.NET

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

cd /hadoop/cdm/bin

# Spark Job name
NAME="CDM-HL7-Process"
now=$(date +"%Y-%m-%d %T")
echo "Date and Time : $now"
Running_Jobs=$NAME".log"
echo "Log File: " $Running_Jobs
rm -f $Running_Jobs

echo "Job verification has started"
ExitCode=0
Counter=1

while [ $Counter -lt 4 ]
do
    echo "Counter #: " $Counter
    # Checking running States
    KeyTab_Check
    timeout 60 yarn application -appStates RUNNING -list | grep "SPARK" > $Running_Jobs
    echo "Log file is created"
    cat $Running_Jobs
    # returnvalue=$(timeout 60 yarn application -appStates RUNNING -list | grep -w ${NAME} | wc -l)
    returnvalue=$(grep -w ${NAME} $Running_Jobs | wc -l)
    if (( $returnvalue > 0 )); then
      ExitCode=0
      break
    else  
      if [[ "$Counter" -eq 3 ]];then
	        echo " Tried for 3 times...";
      else
          echo "$(date): Going to sleep. Will try again after 30 seconds !"
          sleep 30s
      fi
      ExitCode=1
    fi
    let Counter=Counter+1  
done
echo "End of script..."
if [[ "$ExitCode" -eq 1 ]];then
      echo "$(date): ${NAME} is NOT running. Please Investigate !"
      echo "Return value: $returnvalue"
      exit 1
else
      echo "$NAME Running"
      echo "$NAME Verification is completed"
      exit 0
fi
