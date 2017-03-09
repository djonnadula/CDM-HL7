USE hl7;

CREATE EXTERNAL TABLE hl7_zbt_data (
	msh_sending_facility String,
	msh_msg_control_id String,
	pid_medical_record_num String,
	pid_medical_record_urn String,
	pid_pat_account_num String,
	zbt_counter String,
	zbt_types String,
	zbt_transfused_units_number String,
	zbt_unknown String,
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
LOCATION '/user/hive/warehouse/hl7/landing_zone=SEGMENTS/hl7_segment=ZBT';

CREATE EXTERNAL TABLE hl7_zct_data (
	msh_sending_facility String,
	msh_msg_control_id String,
	pid_medical_record_num String,
	pid_medical_record_urn String,
	pid_pat_account_num String,
	zct_counter String,
	zct_count_type String,
	zct_count_one_nurse_one String,
	zct_count_one_nurse_two String,
	zct_count_two_nurse_one String,
	zct_count_two_nurse_two String,
	zct_count_two_comment_one String,
	zct_count_two_comment_one String,
	zct_count_two_correct String,
	zct_count_three_nurse_one String,
	zct_count_three_nurse_two String,
	zct_count_three_comment_one String,
	zct_count_three_comment_two String,
	zct_count_three_correct String,
	zct_count_four_type String,
	zct_count_four_nurse_one String,
	zct_count_four_nurse_two String,
	zct_count_four_comment_one String,
	zct_count_four_comment_two String,
	zct_count_four_correct String,
	zct_unknown String,
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
LOCATION '/user/hive/warehouse/hl7/landing_zone=SEGMENTS/hl7_segment=ZCT';

CREATE EXTERNAL TABLE hl7_zcx_data (
	msh_sending_facility String,
	msh_msg_control_id String,
	pid_medical_record_num String,
	pid_medical_record_urn String,
	pid_pat_account_num String,
	zcx_counter String,
	zcx_complication_type String,
	zcx_complication String,
	zcx_complication_complication_mnemonic String,
	zcx_complication_complication_description String,
	zcx_unknown String,
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
LOCATION '/user/hive/warehouse/hl7/landing_zone=SEGMENTS/hl7_segment=ZCX';

CREATE EXTERNAL TABLE hl7_zdv_data (
	msh_sending_facility String,
	msh_msg_control_id String,
	pid_medical_record_num String,
	pid_medical_record_urn String,
	pid_pat_account_num String,
	zdv_counter String,
	zdv_device String,
	zdv_device_device_mnemonic String,
	zdv_device_device_description String,
	zdv_device_comment String,
	zdv_unknown String,
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
LOCATION '/user/hive/warehouse/hl7/landing_zone=SEGMENTS/hl7_segment=ZDV';

CREATE EXTERNAL TABLE hl7_ze1_data (
	msh_sending_facility String,
	msh_msg_control_id String,
	pid_medical_record_num String,
	pid_medical_record_urn String,
	pid_pat_account_num String,
	zeq_id String,
	zeq_counter String,
	zeq_observation_identifier String,
	zeq_observation_identifier_equipment_query_mnemonic String,
	zeq_observation_idenfitier_query_text String,
	zeq_observation_sub_id String,
	zeq_observation_value String,
	ze1_unknown String,
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
LOCATION '/user/hive/warehouse/hl7/landing_zone=SEGMENTS/hl7_segment=ZE1';

CREATE EXTERNAL TABLE hl7_zeq_data (
	msh_sending_facility String,
	msh_msg_control_id String,
	pid_medical_record_num String,
	pid_medical_record_urn String,
	pid_pat_account_num String,
	zeq_counter String,
	zeq_equipment String,
	zeq_equipment_equipment_mnemonic String,
	zeq_equipment_equipment_description String,
	zeq_unknown String,
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
LOCATION '/user/hive/warehouse/hl7/landing_zone=SEGMENTS/hl7_segment=ZEQ';

CREATE EXTERNAL TABLE hl7_zip_data (
	msh_sending_facility String,
	msh_msg_control_id String,
	pid_medical_record_num String,
	pid_medical_record_urn String,
	pid_pat_account_num String,
	zip_counter String,
	zip_types String,
	zip_types_values String,
	zip_procedure String,
	zip_procedure_invansive_procedure_mnemonic String,
	zip_procedure_invansive_procedure_name String,
	zip_invansive_procedure_performed_by String,
	zip_invansive_procedure_performed_by_surgeon_mnemonic String,
	zip_invansive_procedure_performed_by_last_name String,
	zip_invansive_procedure_performed_by_first_name String,
	zip_user String,
	zip_date_time String,
	zip_quantity String,
	zip_site String,
	zip_site_site_mnemonic String,
	zip_site_site_name String,
	zip_size String,
	zip_first_comment String,
	zip_second_comment String,
	zip_unknown String,
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
LOCATION '/user/hive/warehouse/hl7/landing_zone=SEGMENTS/hl7_segment=ZIP';

CREATE EXTERNAL TABLE hl7_zit_data (
	msh_sending_facility String,
	msh_msg_control_id String,
	pid_medical_record_num String,
	pid_medical_record_urn String,
	pid_pat_account_num String,
	zit_counter String,
	zit_inventory String,
	zit_inventory_inventory_mnemonic String,
	zit_inventory_inventory_name String,
	zit_inventory_type String,
	zit_inventory_type_inventory_type_mnemonic String,
	zit_inventory_type_inventory_type_name String,
	zit_item String,
	zit_item_item_mnemonic String,
	zit_item_item_override_description String,
	zit_item_item_dictionary_description String,
	zit_item_item_dictionary_alternate_description String,
	zit_item_quantity String,
	zit_item_quantity_out String,
	zit_unknown String,
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
LOCATION '/user/hive/warehouse/hl7/landing_zone=SEGMENTS/hl7_segment=ZIT';

CREATE EXTERNAL TABLE hl7_zm0_data (
	msh_sending_facility String,
	msh_msg_control_id String,
	pid_medical_record_num String,
	pid_medical_record_urn String,
	pid_pat_account_num String,
	zm0_counter String,
	zm0_implant_inventory String,
	zm0_implant_inventory_inventory_mnemonic String,
	zm0_implant_inventory_inventory_name String,
	zm0_implant_type String,
	zm0_implant_type_type_mnemonic String,
	zm0_implant_type_type_name String,
	zm0_implant_item String,
	zm0_implant_item_item_mnemonic String,
	zm0_implant_item_item_override_description String,
	zm0_implant_item_item_dictionary_description String,
	zm0_implant_item_item_dictionary_alternate_description String,
	zm0_implant_quantity String,
	zm0_implant_quantity_out String,
	zm0_unknown String,
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
LOCATION '/user/hive/warehouse/hl7/landing_zone=SEGMENTS/hl7_segment=ZM0';

CREATE EXTERNAL TABLE hl7_zou_data (
	msh_sending_facility String,
	msh_msg_control_id String,
	pid_medical_record_num String,
	pid_medical_record_urn String,
	pid_pat_account_num String,
	zou_counter String,
	zou_type String,
	zou_output String,
	zou_output_date_time String,
	zou_output_site String,
	zou_output_site_site_mnemonic String,
	zou_output_site_site_name String,
	zou_output_type String,
	zou_output_type_output_type_mnemonic String,
	zou_output_type_output_type_name String,
	zou_output_volume String,
	zou_output_user String,
	zou_unknown String,
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
LOCATION '/user/hive/warehouse/hl7/landing_zone=SEGMENTS/hl7_segment=ZOU';

CREATE EXTERNAL TABLE hl7_zpa_data (
	msh_sending_facility String,
	msh_msg_control_id String,
	pid_medical_record_num String,
	pid_medical_record_urn String,
	pid_pat_account_num String,
	zpa_counter String,
	zpa_par_date_time String,
	zpa_par_user_mnemonic String,
	zpa_activity_par String,
	zpa_respiration_par String,
	zpa_circulation_par String,
	zpa_consciousness_par String,
	zpa_color_par String,
	zpa_total_par String,
	zpa_unknown String,
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
LOCATION '/user/hive/warehouse/hl7/landing_zone=SEGMENTS/hl7_segment=ZPA';

CREATE EXTERNAL TABLE hl7_zps_data (
	msh_sending_facility String,
	msh_msg_control_id String,
	pid_medical_record_num String,
	pid_medical_record_urn String,
	pid_pat_account_num String,
	zps_counter String,
	zps_position String,
	zps_position_position_mnemonic String,
	zps_position_position_description String,
	zps_unknown String,
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
LOCATION '/user/hive/warehouse/hl7/landing_zone=SEGMENTS/hl7_segment=ZPS';

CREATE EXTERNAL TABLE hl7_ztx_data (
	msh_sending_facility String,
	msh_msg_control_id String,
	pid_medical_record_num String,
	pid_medical_record_urn String,
	pid_pat_account_num String,
	ztx_counter String,
	ztx_type_of_text_comment_values String,
	ztx_text_line String,
	ztx_unknown String,
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
LOCATION '/user/hive/warehouse/hl7/landing_zone=SEGMENTS/hl7_segment=ZTX';