# Regular Partitioner Query
A daily Oozie job that updates Hive partitions in the hl7.hl7_all_proc_rejected table with yesterday's partition directories. 

##### Features 
1.  Schedule: once per day at 6 AM
2. Runs on the following directories
   1. hdfs://nameservice1/user/hive/warehouse/hl7.db/hl7_all_proc_rejected/message_date=${MESSAGE_DATE}
3. Sends an email in case of failure.

##### Deployment
1. Run bin/prod-oozie-regular-partitioner-submit.sh or bin/qa-oozie-regular-partitioner-submit.sh depending on environment

##### Notes

##### TODOs
