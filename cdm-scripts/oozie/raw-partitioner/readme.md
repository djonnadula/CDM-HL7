# Partitioner
A daily Oozie job that updates Hive partitions in the hl7.hl7_raw_data table with yesterday's partition directories. 

##### Features 
1. Schedule: once per day at 6 AM
2. Runs on the following directories
    1. hdfs://nameservice1/user/hive/warehouse/hl7.db/raw/message_type=adt/message_date=${MESSAGE_DATE}
    2. hdfs://nameservice1/user/hive/warehouse/hl7.db/raw/message_type=mdm/message_date=${MESSAGE_DATE}
    3. hdfs://nameservice1/user/hive/warehouse/hl7.db/raw/message_type=oru/message_date=${MESSAGE_DATE}
3. Sends an email in case of failure.

##### Deployment
1. Generate a new hive-config.xml from production cluster (hive-config.xml is hive-site.xml - had problems using hive-site.xml as file name in Hue)
2. Put the `lib`, `coordinator.xml`, `workflow.xml` into an HDFS directory
3. Update the `oozie.coord.application.path=${nameNode}/` property in each prod*.properties files to point to this directory
4. Update the `workflowPath=${nameNode}/` property in each prod*.properties files to point to this directory
5. Update the `SCRIPT_PATH=${nameNode}/` property in each prod*.properties files to point where the `lib/hive_raw_data_partitioner.hql` is stored in hdfs
6. Make sure that the executing user can write to all `/clone`, `/crush` directories
7. Run the `prod-oozie-partitioner-submit.sh` script.
8. Check the Hue Oozie coordinator UI to see that 3 jobs have been submitted
9. Success, the jobs have been scheduled and submitted.

##### Notes

##### TODOs

