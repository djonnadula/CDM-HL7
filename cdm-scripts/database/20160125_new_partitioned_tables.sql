USE hl7;

CREATE EXTERNAL TABLE hl7.hl7_audit_data (
	process_name STRING,
	msg_control_id STRING,
	msg_create_date_time STRING,
	patient_mrn STRING,
	patient_urn STRING,
	patient_account_num STRING,
	etl_firstinsert_datetime STRING
)
PARTITIONED BY (
	message_type String,
	transaction_date String
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS SEQUENCEFILE
LOCATION 'hdfs://nameservice1/user/hive/warehouse/hl7.db/dev/landing_zone=AUDIT';

CREATE EXTERNAL TABLE hl7.hl7_json_data(
	json_data STRING
)
PARTITIONED BY (
	message_type String,
	transaction_date String
)
WITH SERDEPROPERTIES ('serialization.format'='1')
STORED AS SEQUENCEFILE
LOCATION 'hdfs://nameservice1/user/hive/warehouse/hl7.db/dev/landing_zone=JSON'

CREATE EXTERNAL TABLE hl7.hl7_raw_data (
	msg_data STRING
)
PARTITIONED BY (
	message_type STRING,
	transaction_date STRING
)
STORED AS SEQUENCEFILE
LOCATION 'hdfs://nameservice1/user/hive/warehouse/hl7.db/dev/landing_zone=RAW'

CREATE EXTERNAL TABLE hl7.hl7_all_proc_rejected (
	process_name String,
	msg_control_id String,
	msg_create_date_time String,
	patient_mrn String,
	patient_urn String,
	patient_account_num String,
	etl_firstinsert_datetime String,
	reject_reason String,
	rejected_message_data String,
	rejected_message_data_extn String
)
PARTITIONED BY (
	 message_type String,
	 transaction_date String
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS SEQUENCEFILE
LOCATION 'hdfs://nameservice1/user/hive/warehouse/hl7.db/dev/landing_zone=REJECT' ;
