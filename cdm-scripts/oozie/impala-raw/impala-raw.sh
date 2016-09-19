#!/usr/bin/env bash

kerberosReAuth() {
    kinitcmd="kinit -k -t /home/corpsrvcdmbtch_qa/corpsrvcdmbtch_qa.keytab CorpSRVCDMBtch_QA@HCA.CORPAD.NET"
    echo "Checking for Kerberos ticket..."
    klist | grep HCA.CORPAD.NET
    ticketExists=$?
    if [[ "$ticketExists" -eq 0 ]];then
    	echo "You got a ticket, So no kinit stuff...";
    else
        echo "No ticket, So kinit'ing...";
        $kinitcmd
        kinitSuccessful=$?
        if [[ "$kinitSuccessful" -eq 0 ]];then
            echo "kinit successful."
        else
            echo "Failed to do kinit, exiting.."
            exit 1
        fi
    fi
}

FILE=$1
INVOKE_IMPALA=`/usr/bin/impala-shell -i impaladev.hca.corpad.net -k --ssl -f ${FILE}`

ALTER TABLE hl7_raw_test add partition (message_type='SIU') location '/user/hive/warehouse/hl7.db/hl7_raw_test/message_type=SIU/message_date=2016-09-06';