USE hl7;

DROP TABLE hl7_zm1_data;

CREATE EXTERNAL TABLE hl7_zm1_data (
	msh_sending_facility String,
	msh_msg_control_id String,
	pid_medical_record_num String,
	pid_medical_record_urn String,
	pid_pat_account_num String,
	zm1_set_id String,
    zm1_insurance String,
    zm1_query_data String,
    zm1_query_data_query_code String,
    zm1_query_data_query_text String,
    zm1_query_data_query_response String,
    zm1_counter String,
    zm1_implant_manufacturer String,
    zm1_implant_manufacturer_manufacturer_mnemonic String,
    zm1_implant_manufacturer_manufacturer_name String,
    zm1_manufacturer_com String,
    zm1_implant_surgeon String,
    zm1_implant_surgeon_surgeon_mnemonic String,
    zm1_implant_surgeon_last_name String,
    zm1_implant_surgeon_first_name String,
    zm1_catalog_number String,
    zm1_batch_number String,
    zm1_count String,
    zm1_culture String,
    zm1_expiration_date String,
    zm1_lot_number String,
    zm1_serial_number String,
    zm1_implant_site String,
    zm1_implant_size String,
    zm1_solution_expiration_date String,
    zm1_solution_inventory String,
    zm1_solution_lot String,
    zm1_solution_prep_by String,
    zm1_solution_prep_date_time String,
    zm1_solution_comment String,
    zm1_opened_by_user String,
    zm1_opened_date String,
    zm1_opened_time String,
    zm1_prepped_by String,
    zm1_prepped_date String,
    zm1_prepped_time String,
    zm1_unknown String,
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
LOCATION '/user/hive/warehouse/hl7.db/dev/landing_zone=SEGMENTS/hl7_segment=ZM1';

DROP TABLE hl7_zm1_data;

CREATE EXTERNAL TABLE hl7_zm1_data (
	msh_sending_facility String,
	msh_msg_control_id String,
	pid_medical_record_num String,
	pid_medical_record_urn String,
	pid_pat_account_num String,
	zm1_set_id String,
    zm1_insurance String,
    zm1_query_data String,
    zm1_query_data_query_code String,
    zm1_query_data_query_text String,
    zm1_query_data_query_response String,
    zm1_counter String,
    zm1_implant_manufacturer String,
    zm1_implant_manufacturer_manufacturer_mnemonic String,
    zm1_implant_manufacturer_manufacturer_name String,
    zm1_manufacturer_com String,
    zm1_implant_surgeon String,
    zm1_implant_surgeon_surgeon_mnemonic String,
    zm1_implant_surgeon_last_name String,
    zm1_implant_surgeon_first_name String,
    zm1_catalog_number String,
    zm1_batch_number String,
    zm1_count String,
    zm1_culture String,
    zm1_expiration_date String,
    zm1_lot_number String,
    zm1_serial_number String,
    zm1_implant_site String,
    zm1_implant_size String,
    zm1_solution_expiration_date String,
    zm1_solution_inventory String,
    zm1_solution_lot String,
    zm1_solution_prep_by String,
    zm1_solution_prep_date_time String,
    zm1_solution_comment String,
    zm1_opened_by_user String,
    zm1_opened_date String,
    zm1_opened_time String,
    zm1_prepped_by String,
    zm1_prepped_date String,
    zm1_prepped_time String,
    zm1_unknown String,
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
LOCATION '/user/hive/warehouse/hl7_qa/landing_zone=SEGMENTS/hl7_segment=ZM1';

DROP TABLE hl7_zm1_data;

CREATE EXTERNAL TABLE hl7_zm1_data (
	msh_sending_facility String,
	msh_msg_control_id String,
	pid_medical_record_num String,
	pid_medical_record_urn String,
	pid_pat_account_num String,
	zm1_set_id String,
    zm1_insurance String,
    zm1_query_data String,
    zm1_query_data_query_code String,
    zm1_query_data_query_text String,
    zm1_query_data_query_response String,
    zm1_counter String,
    zm1_implant_manufacturer String,
    zm1_implant_manufacturer_manufacturer_mnemonic String,
    zm1_implant_manufacturer_manufacturer_name String,
    zm1_manufacturer_com String,
    zm1_implant_surgeon String,
    zm1_implant_surgeon_surgeon_mnemonic String,
    zm1_implant_surgeon_last_name String,
    zm1_implant_surgeon_first_name String,
    zm1_catalog_number String,
    zm1_batch_number String,
    zm1_count String,
    zm1_culture String,
    zm1_expiration_date String,
    zm1_lot_number String,
    zm1_serial_number String,
    zm1_implant_site String,
    zm1_implant_size String,
    zm1_solution_expiration_date String,
    zm1_solution_inventory String,
    zm1_solution_lot String,
    zm1_solution_prep_by String,
    zm1_solution_prep_date_time String,
    zm1_solution_comment String,
    zm1_opened_by_user String,
    zm1_opened_date String,
    zm1_opened_time String,
    zm1_prepped_by String,
    zm1_prepped_date String,
    zm1_prepped_time String,
    zm1_unknown String,
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
LOCATION '/user/hive/warehouse/hl7_reg_test/landing_zone=SEGMENTS/hl7_segment=ZM1';