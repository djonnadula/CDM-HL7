use hl7;

DROP TABLE hl7_all_proc_rejected;

CREATE EXTERNAL TABLE `hl7_all_proc_rejected`(
  `process_name` string,
  `msg_control_id` string,
  `msg_create_date_time` string,
  `patient_mrn` string,
  `patient_urn` string,
  `patient_account_num` string,
  `reject_reason` string,
  `rejected_message_data` string,
  `etl_firstinsert_datetime` string)
PARTITIONED BY (
  `message_type` string,
  `transaction_date` string)
ROW FORMAT SERDE
  'com.amazon.elasticmapreduce.JsonSerde'
WITH SERDEPROPERTIES (
  'paths'='process_name, msg_control_id, msg_create_date_time, patient_mrn, patient_urn, patient_account_num,  reject_reason, rejected_message_data, etl_firstinsert_datetime')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.SequenceFileInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat'
LOCATION
  'hdfs://nameservice1/user/hive/warehouse/hl7.db/landing_zone=REJECT'