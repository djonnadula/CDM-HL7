USE hl7;

DROP TABLE hl7.hl7_audit_data;

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

DROP TABLE hl7.hl7_json_data;

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

DROP TABLE hl7.hl7_raw_data;

CREATE EXTERNAL TABLE hl7.hl7_raw_data (
	msg_data STRING
)
PARTITIONED BY (
	message_type STRING,
	transaction_date STRING
)
STORED AS SEQUENCEFILE
LOCATION 'hdfs://nameservice1/user/hive/warehouse/hl7.db/dev/landing_zone=RAW'

use cdm_scri;

drop table nlp_patient_id_data;

CREATE EXTERNAL TABLE nlp_patient_id_data (
	msh_msg_control_id STRING,
	msh_sending_facility STRING,
	msh_msg_create_date_time STRING,
	txa_unique_document_num STRING,
	txa_unique_document_num_entity_id STRING,
	txa_unique_document_num_namespace_id STRING,
	txa_origination_date_time STRING,
	txa_transcription_date_time STRING,
	msh_msg_type_message_code STRING,
	msh_msg_type_trigger_event STRING,
	pid_pat_account_num STRING,
	pid_pat_id_list_id_num STRING,
	pid_pat_id_list STRING,
	pid_medical_record_num STRING,
	pid_alt_pat_id_id_num STRING,
	pid_alt_pat_id STRING,
	pid_medical_record_urn STRING,
	pid_birth_date_time STRING,
	pid_pat_name_last_name STRING,
	pid_pat_name_first_name STRING,
	pid_sex STRING,
	pv1_adm_date_time STRING,
	pv1_dchg_date_time STRING,
	txa_originator_person_id STRING,
	txa_originator_last_name STRING,
	txa_originator_first_name STRING,
	txa_originator_middle_name STRING,
	txa_originator_suffix STRING,
	txa_originator_prefix STRING,
	txa_originator_degree STRING,
	txa_filler_order_num STRING,
	txa_document_type_identifier STRING,
	txa_document_type_text STRING,
	txa_name_of_coding_sys STRING,
	txa_document_completion_status STRING,
	obx_observation_value STRING,
	etl_firstinsert_datetime STRING
)
PARTITIONED BY (
	 message_type String,
	 transaction_date String
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS SEQUENCEFILE
LOCATION 'hdfs://nameservice1/user/hive/warehouse/cdm_scri/landing_zone=ADHOC-CDI-DELIMITED';

drop table scri_patient_id_history;

CREATE EXTERNAL TABLE scri_patient_id_history (
	msh_msg_control_id STRING,
	msh_sending_facility STRING,
	txa_origination_date_time STRING,
	txa_transcription_date_time STRING,
	msh_msg_type_message_code STRING,
	msh_msg_type_trigger_event STRING,
	pid_pat_account_num STRING,
	pid_medical_record_num STRING,
	pid_medical_record_urn STRING,
	pid_birth_date_time STRING,
	pid_pat_name_last_name STRING,
	pid_pat_name_first_name STRING,
	pid_sex STRING,
	pv1_adm_date_time STRING,
	txa_originator_person_id STRING,
	txa_originator_last_name STRING,
	txa_originator_first_name STRING,
	txa_originator_middle_name STRING,
	txa_originator_suffix STRING,
	txa_originator_prefix STRING,
	txa_originator_degree STRING,
	txa_filler_order_num STRING,
	txa_document_type_identifier STRING,
	txa_document_type_text STRING,
	txa_document_completion_status STRING,
	obx_observation_value STRING
)
PARTITIONED BY (
	 message_type String,
	 transaction_date String
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS SEQUENCEFILE
LOCATION 'hdfs://nameservice1/user/hive/warehouse/cdm_scri/landing_zone=ADHOC-SCRIPATIDHIST-DELIMITED';
