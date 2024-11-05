#!/bin/bashd

AVRO_TO_PARQUET_HOME=/home/hadoop/cdm/scripts/AvroToParquet/
cd $AVRO_TO_PARQUET_HOME

#DEFINE AN ARRAY OF DATES (YYYY-MM-DD)
DATE_ARR=("2016-06-14" "2016-06-21") 

#DATE=$1
#echo $DATE
HDFS_DIR_ROOT="/user/hive/warehouse/hl7.db"
AVRO_DATA_DIR="hl7_avro_data"
AVRO_TODAY_DIR="hl7_avro_data_today"
AVRO_HISTORY_DIR="hl7_avro_data_history"

for DATE in "${DATE_ARR[@]}"
do
    timeout -s9 300s kinit -k -t /home/corpsrvcdmbtch_qa/corpsrvcdmbtch_qa.keytab CorpSRVCDMBtch_QA@.NET
    echo "Current directory date: $DATE"
    echo "hdfs dfs -mv $HDFS_DIR_ROOT/$AVRO_DATA_DIR/$DATE $HDFS_DIR_ROOT/$AVRO_TODAY_DIR/"
    hdfs dfs -mv $HDFS_DIR_ROOT/$AVRO_DATA_DIR/$DATE $HDFS_DIR_ROOT/$AVRO_TODAY_DIR/

    hive -f HL7AvroToParquet_history.hql

    timeout -s9 300s kinit -k -t /home/corpsrvcdmbtch_qa/corpsrvcdmbtch_qa.keytab CorpSRVCDMBtch_QA@.NET
    #hdfs dfs -find /user/hive/warehouse/hl7.db/hl7_avro_data/ -type d -ctime +5 -exec mv '{}' /user/hive/warehouse/hl7.db/hl7_avro_data_history/ \;

    if [ $? -eq 0 ]; then	
        echo "Hive query successful. Moving $DATE files to history"
        echo "hdfs dfs -mv $HDFS_DIR_ROOT/$AVRO_TODAY_DIR/$DATE $HDFS_DIR_ROOT/$AVRO_HISTORY_DIR/"
        hdfs dfs -mv $HDFS_DIR_ROOT/$AVRO_TODAY_DIR/$DATE $HDFS_DIR_ROOT/$AVRO_HISTORY_DIR/
    else 
        echo "Hive query failed. Return val $?. Files not moved to history"
    fi
    echo "Completed $DATE"
done

