USE hl7;

CREATE EXTERNAL TABLE hl7_zst_data (
segment_type String,
msh_sending_facility String,
msh_msg_control_id String,
pid_medical_record_num String,
pid_medical_record_urn String,
pid_pat_account_num String,
zst_counter String,
zst_staff_type String,
zst_staff_identifier String,
zst_staff_identifier_person_id String,
zst_staff_identifier_last_name String,
zst_staff_identifier_first_name String,
zst_comment String,
zst_in_out_range String,
zst_in_out_range_sequence_num String,
zst_in_out_range_range_date String,
zst_in_out_range_range_in_time String,
zst_in_out_range_range_out_time String,
etl_firstinsert_datetime String,
field_sequence_num String
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS SEQUENCEFILE
LOCATION '/user/hive/warehouse/hl7/hl7_all_segment_data';

CREATE VIEW vw_hl7_zst_data AS SELECT * FROM hl7_zst_data WHERE segment_type='ZST';