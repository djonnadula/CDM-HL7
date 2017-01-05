USE hl7;

CREATE EXTERNAL TABLE hl7_zrx_data (
segment_type String,
msh_sending_facility String,
msh_msg_control_id String,
pid_medical_record_num String,
pid_medical_record_urn String,
pid_pat_account_num String,
zrx_counter STRING,
zrx_dispense_code STRING,
zrx_medication_type STRING,
zrx_cycle_date_time STRING,
zrx_medication STRING,
zrx_medication_medication_code STRING,
zrx_medication_medication_name STRING,
zrx_bottle_type STRING,
zrx_date_time STRING,
zrx_dose String,
zrx_route String,
zrx_route_route_code String,
zrx_route_route_name String,
zrx_site String,
zrx_site_site_code String,
zrx_site_site_name String,
zrx_user String,
zrx_dosage_units String,
zrx_ordering_physician String,
zrx_ordering_physician_person_id String,
zrx_ordering_physician_last_name String,
zrx_ordering_physician_first_name String,
zrx_comment String,
zrx_second_comment String,
etl_firstinsert_datetime String,
field_sequence_num String
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS SEQUENCEFILE
LOCATION '/user/hive/warehouse/hl7.db/hl7_all_segment_data';
