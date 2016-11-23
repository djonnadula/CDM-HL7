use cdm_scri;

CREATE EXTERNAL TABLE scri_patient_id_json_history (
	msh_sending_facility String,
	msh_msg_control_id String,
	pid_medical_record_num String,
	pid_medical_record_urn String,
	pid_pat_account_num String,
	txa_origination_date_time STRING,
	txa_transcription_date_time STRING,
	msh_msg_type_message_code STRING,
	msh_msg_type_trigger_event STRING,
	pid_birth_date_time STRING,
	pid_pat_name_last_name STRING,
	pid_pat_name_first_name STRING,
	pid_sex STRING,
	pv1_adm_date_time STRING,
	pv1_dchg_date_time STRING,
	txa_originator_code_name_person_id STRING,
	txa_originator_code_name_last_name STRING,
	txa_originator_code_name_first_name STRING,
	txa_originator_code_name_middle_name STRING,
	txa_originator_code_name_suffix STRING,
	txa_originator_code_name_prefix STRING,
	txa_originator_code_name_degree STRING,
	txa_filler_order_num STRING,
	txa_document_type_primary_id STRING,
	txa_document_type_text STRING,
	txa_document_type_name_of_coding_sys STRING,
	txa_document_completion_status STRING,
	obx_observation_value STRING,
	pid_pat_social_security_num STRING,
	pid_pat_address_street_address1 STRING,
	pid_pat_address_street_address2 STRING,
	pid_pat_address_city STRING,
	pid_pat_address_state_province STRING,
	pid_pat_address_zip_postal_code STRING,
	etl_firstinsert_datetime STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS SEQUENCEFILE
LOCATION '/user/hive/warehouse/cdm_scri/scri_pat_id_json_history';