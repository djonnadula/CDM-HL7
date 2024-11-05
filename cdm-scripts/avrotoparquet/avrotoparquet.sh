#!/bin/bashd

kerberosReAuth() {
    kinitcmd="kinit -k -t /home/corpsrvcdmbtch_qa/corpsrvcdmbtch_qa.keytab CorpSRVCDMBtch_QA@.NET"
    echo "Checking for Kerberos ticket..."
    klist | grep .NET
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

AVRO_TO_PARQUET_HOME=/home/hadoop/cdm/scripts/AvroToParquet/
cd $AVRO_TO_PARQUET_HOME

$kinitcmd

YESTERDAY=`date -d "-1 days" +"%Y-%m-%d"`
HDFS_DIR_ROOT="/user/hive/warehouse/hl7.db"
AVRO_DATA_DIR="hl7_avro_data"
AVRO_TODAY_DIR="hl7_avro_data_today"
AVRO_HISTORY_DIR="hl7_avro_data_history"

echo "hdfs dfs -mv $HDFS_DIR_ROOT/$AVRO_DATA_DIR/$YESTERDAY $HDFS_DIR_ROOT/$AVRO_TODAY_DIR/"
hdfs dfs -mv $HDFS_DIR_ROOT/$AVRO_DATA_DIR/$YESTERDAY $HDFS_DIR_ROOT/$AVRO_TODAY_DIR/

$kinitcmd
hivetoparquet="hive -f HL7AvroToParquet.hql"
$hivetoparquet
hiveComplete=$?

if [[ "$hiveComplete" -eq 0 ]]; then
    $kinitcmd
    echo "Hive query successful. Moving $YESTERDAY files to history"
    hdfs dfs -mv $HDFS_DIR_ROOT/$AVRO_TODAY_DIR/$YESTERDAY $HDFS_DIR_ROOT/$AVRO_HISTORY_DIR/
else
    echo "Hive query failed. Return val $?. Files not moved to history"
fi
