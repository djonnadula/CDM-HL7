set hive.input.dir.recursive=true;
set hive.mapred.supports.subdirectories=true;
set hive.supports.subdirectories=true;
set mapred.input.dir.recursive=true;

set hive.exec.compress.output=true; 
set parquet.compression=SNAPPY; 

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

SET hive.exec.max.dynamic.partitions=10000000;
SET hive.exec.max.dynamic.partitions.pernode=10000000;

SET mapreduce.map.memory.mb=8192; 
SET mapreduce.map.java.opts=-Xmx6144m;

SET mapreduce.reduce.memory.mb=4096; 
SET mapreduce.reduce.java.opts=-Xmx3276m;

insert into table hl7.hl7_parquet_data partition(message_type,source_facility,message_date)
select parse_result, message_control_id, message_version, message_type_trigger, message_segments, message_time, dbmid, dbm_message_time,
patient_name, patient_alt_id, patient_dob, obx_observation_text, obx_text_length, rxe_admin_instructions, nte_comments, generic_fields,
raw_record, current_timestamp as firstinsertdatetime, SUBSTR(message_type_trigger,1,3) AS message_type, source_facility,
from_unixtime(unix_timestamp(substring(message_time, 1, 8), 'yyyyMMdd'), 'yyyy-MM-dd') as message_date
from hl7.hl7_avro_data_today
where message_type_trigger like 'ADT%'
;

insert into table hl7.hl7_parquet_data partition(message_type,source_facility,message_date)
select parse_result, message_control_id, message_version, message_type_trigger, message_segments, message_time, dbmid, dbm_message_time,
patient_name, patient_alt_id, patient_dob, obx_observation_text, obx_text_length, rxe_admin_instructions, nte_comments, generic_fields,
raw_record, current_timestamp as firstinsertdatetime, SUBSTR(message_type_trigger,1,3) AS message_type, source_facility,
from_unixtime(unix_timestamp(substring(message_time, 1, 8), 'yyyyMMdd'), 'yyyy-MM-dd') as message_date
from hl7.hl7_avro_data_today
where message_type_trigger like 'MDM%'
;


insert into table hl7.hl7_parquet_data partition(message_type,source_facility,message_date)
select parse_result, message_control_id, message_version, message_type_trigger, message_segments, message_time, dbmid, dbm_message_time,
patient_name, patient_alt_id, patient_dob, obx_observation_text, obx_text_length, rxe_admin_instructions, nte_comments, generic_fields,
raw_record, current_timestamp as firstinsertdatetime, SUBSTR(message_type_trigger,1,3) AS message_type, source_facility,
from_unixtime(unix_timestamp(substring(message_time, 1, 8), 'yyyyMMdd'), 'yyyy-MM-dd') as message_date
from hl7.hl7_avro_data_today
where message_type_trigger like 'ORM%'
;

insert into table hl7.hl7_parquet_data partition(message_type,source_facility,message_date)
select parse_result, message_control_id, message_version, message_type_trigger, message_segments, message_time, dbmid, dbm_message_time,
patient_name, patient_alt_id, patient_dob, obx_observation_text, obx_text_length, rxe_admin_instructions, nte_comments, generic_fields,
raw_record, current_timestamp as firstinsertdatetime, SUBSTR(message_type_trigger,1,3) AS message_type, source_facility,
from_unixtime(unix_timestamp(substring(message_time, 1, 8), 'yyyyMMdd'), 'yyyy-MM-dd') as message_date
from hl7.hl7_avro_data_today
where message_type_trigger like 'ORU%'
;

insert into table hl7.hl7_parquet_data partition(message_type,source_facility,message_date)
select parse_result, message_control_id, message_version, message_type_trigger, message_segments, message_time, dbmid, dbm_message_time,
patient_name, patient_alt_id, patient_dob, obx_observation_text, obx_text_length, rxe_admin_instructions, nte_comments, generic_fields,
raw_record, current_timestamp as firstinsertdatetime, SUBSTR(message_type_trigger,1,3) AS message_type, source_facility,
from_unixtime(unix_timestamp(substring(message_time, 1, 8), 'yyyyMMdd'), 'yyyy-MM-dd') as message_date
from hl7.hl7_avro_data_today
where message_type_trigger like 'IPL%'
;

insert into table hl7.hl7_parquet_data partition(message_type,source_facility,message_date)
select parse_result, message_control_id, message_version, message_type_trigger, message_segments, message_time, dbmid, dbm_message_time,
patient_name, patient_alt_id, patient_dob, obx_observation_text, obx_text_length, rxe_admin_instructions, nte_comments, generic_fields,
raw_record, current_timestamp as firstinsertdatetime, SUBSTR(message_type_trigger,1,3) AS message_type, source_facility,
from_unixtime(unix_timestamp(substring(message_time, 1, 8), 'yyyyMMdd'), 'yyyy-MM-dd') as message_date
from hl7.hl7_avro_data_today
where message_type_trigger like 'PPR%'
;

insert into table hl7.hl7_parquet_data partition(message_type,source_facility,message_date)
select parse_result, message_control_id, message_version, message_type_trigger, message_segments, message_time, dbmid, dbm_message_time,
patient_name, patient_alt_id, patient_dob, obx_observation_text, obx_text_length, rxe_admin_instructions, nte_comments, generic_fields,
raw_record, current_timestamp as firstinsertdatetime, SUBSTR(message_type_trigger,1,3) AS message_type, source_facility,
from_unixtime(unix_timestamp(substring(message_time, 1, 8), 'yyyyMMdd'), 'yyyy-MM-dd') as message_date
from hl7.hl7_avro_data_today
where message_type_trigger like 'RAS%'
;

insert into table hl7.hl7_parquet_data partition(message_type,source_facility,message_date)
select parse_result, message_control_id, message_version, message_type_trigger, message_segments, message_time, dbmid, dbm_message_time,
patient_name, patient_alt_id, patient_dob, obx_observation_text, obx_text_length, rxe_admin_instructions, nte_comments, generic_fields,
raw_record, current_timestamp as firstinsertdatetime, SUBSTR(message_type_trigger,1,3) AS message_type, source_facility,
from_unixtime(unix_timestamp(substring(message_time, 1, 8), 'yyyyMMdd'), 'yyyy-MM-dd') as message_date
from hl7.hl7_avro_data_today
where message_type_trigger like 'RDE%'
;

insert into table hl7.hl7_parquet_data partition(message_type,source_facility,message_date)
select parse_result, message_control_id, message_version, message_type_trigger, message_segments, message_time, dbmid, dbm_message_time,
patient_name, patient_alt_id, patient_dob, obx_observation_text, obx_text_length, rxe_admin_instructions, nte_comments, generic_fields,
raw_record, current_timestamp as firstinsertdatetime, SUBSTR(message_type_trigger,1,3) AS message_type, source_facility,
from_unixtime(unix_timestamp(substring(message_time, 1, 8), 'yyyyMMdd'), 'yyyy-MM-dd') as message_date
from hl7.hl7_avro_data_today
where message_type_trigger like 'SIU%'
;

insert into table hl7.hl7_parquet_data partition(message_type,source_facility,message_date)
select parse_result, message_control_id, message_version, message_type_trigger, message_segments, message_time, dbmid, dbm_message_time,
patient_name, patient_alt_id, patient_dob, obx_observation_text, obx_text_length, rxe_admin_instructions, nte_comments, generic_fields,
raw_record, current_timestamp as firstinsertdatetime, SUBSTR(message_type_trigger,1,3) AS message_type, source_facility,
from_unixtime(unix_timestamp(substring(message_time, 1, 8), 'yyyyMMdd'), 'yyyy-MM-dd') as message_date
from hl7.hl7_avro_data_today
where message_type_trigger like 'VXU%'
;

--TEMPORARILY WRITING TO BOTH TABLES
--TODO: CLEAN UP OLD TABLE (hl7_parquet_data)
insert into table hl7.hl7_parquet_data_new_partition partition(message_date)
select parse_result, message_control_id, message_version, message_type_trigger, message_segments, message_time, dbmid, dbm_message_time, 
patient_name, patient_alt_id, patient_dob, obx_observation_text, obx_text_length, rxe_admin_instructions, nte_comments, generic_fields, 
raw_record, current_timestamp as firstinsertdatetime, SUBSTR(message_type_trigger,1,3) AS message_type, source_facility, 
from_unixtime(unix_timestamp(substring(message_time, 1, 8), 'yyyyMMdd'), 'yyyy-MM-dd') as message_date
from hl7.hl7_avro_data_today
;


