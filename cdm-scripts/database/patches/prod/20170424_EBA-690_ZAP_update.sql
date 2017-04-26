USE hl7;

CREATE EXTERNAL TABLE hl7_zap_data (
	msh_sending_facility String,
	msh_msg_control_id String,
	pid_medical_record_num String,
	pid_medical_record_urn String,
	pid_pat_account_num String,
	zap_set_id String,
	zap_other_dictated_by_signed_date_time String,
	zap_other_dictated_by_signed_date_time_signed_by_provider_mnemonic String,
	zap_other_dictated_by_signed_date_time_last_name String,
	zap_other_dictated_by_signed_date_time_first_name String,
	zap_other_dictated_by_signed_date_time_middle_name String,
	zap_other_dictated_by_signed_date_time_suffix String,
	zap_other_dictated_by_signed_date_time_title String,
	zap_other_dictated_by_signed_date_time_degree String,
	zap_other_dictated_by_signed_date_time_signed_date_and_time String,
	zap_unknown String,
	etl_firstinsert_datetime String,
	field_sequence_num String
)
PARTITIONED BY (
	message_type String,
	transaction_date String
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS SEQUENCEFILE
LOCATION '/user/hive/warehouse/hl7/landing_zone=SEGMENTS/hl7_segment=ZAP';