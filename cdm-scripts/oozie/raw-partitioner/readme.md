# Raw Partitioner
A daily Oozie job that updates Hive partitions in the hl7.hl7_raw_data table with yesterday's partition directories. 

##### Features 
1. Schedule: once per day at 6 AM
2. Runs on the following directories
    1. hdfs://nameservice1/user/hive/warehouse/hl7.db/raw/message_type=adt/message_date=${MESSAGE_DATE}
    2. hdfs://nameservice1/user/hive/warehouse/hl7.db/raw/message_type=mdm/message_date=${MESSAGE_DATE}
    3. hdfs://nameservice1/user/hive/warehouse/hl7.db/raw/message_type=oru/message_date=${MESSAGE_DATE}
3. Sends an email in case of failure.

##### Deployment
1. Run bin/prod-oozie-raw-partitioner-submit.sh or bin/qa-oozie-raw-partitioner-submit.sh depending on environment

##### Notes

##### TODOs

