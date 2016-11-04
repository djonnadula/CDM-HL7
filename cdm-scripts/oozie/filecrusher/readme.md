# Filecrusher
A daily Oozie job that merges all small files into larger HDFS friendly files.

##### Features 
1. Schedule: once per day at 5 AM
2. This script will move the crushed files to a `/crush` directory and move the original files to a `/clone` 
directory to mitigate risk of losing data if a failure occurs.
3. This script will create any `/crush` or `/clone` directories if they do not already exist.
4. Runs for the previous day if a date is required for the directory structure (raw)
5. After the filecrusher completes, it moves the data from `/crush` to the source directory
6. Runs on the following directories
    1. hdfs://nameservice1/user/hive/warehouse/hl7.db/hl7_all_proc_rejected
    2. hdfs://nameservice1/user/hive/warehouse/hl7.db/hl7_all_segment_data
    3. hdfs://nameservice1/user/hive/warehouse/hl7.db/hl7_audit_data
    4. hdfs://nameservice1/user/hive/warehouse/hl7.db/hl7_json_data
    5. hdfs://nameservice1/user/hive/warehouse/hl7.db/nlp_patient_id_data
    6. hdfs://nameservice1/user/hive/warehouse/hl7.db/raw/message_type=adt/message_date=${MESSAGE_DATE}
    7. hdfs://nameservice1/user/hive/warehouse/hl7.db/raw/message_type=mdm/message_date=${MESSAGE_DATE}
    8. hdfs://nameservice1/user/hive/warehouse/hl7.db/raw/message_type=oru/message_date=${MESSAGE_DATE}
7. Sends an email in case of failure.

##### Deployment
1. Run bin/prod-oozie-filecrusher-submit.sh or bin/qa-oozie-filecrusher-submit.sh depending on environment

##### Notes
* This job only moves and crushes *.snappy files. In a future iteration the logic will be based on file size, not name.

##### TODOs
1. Update script logic to check for file size, not file name (*.snappy)
