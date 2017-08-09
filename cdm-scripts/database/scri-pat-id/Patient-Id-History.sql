use cdm_scri;

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=10000;
set hive.stats.autogather = false;

Drop  Table if exists cdm_scri.scri_patient_id_history_3 PURGE;

CREATE EXTERNAL TABLE cdm_scri.scri_patient_id_history_3(
  msh_msg_control_id STRING COMMENT 'Unique id for the message provided by the source clinical system and BizTalk.',
  msh_sending_facility STRING COMMENT 'Mnemonic that is associated to the sending facility of the message',
  pid_medical_record_num STRING COMMENT 'MRN associated to the message',
  pid_medical_record_urn STRING COMMENT 'URN associated to the message',
  pid_pat_account_num STRING COMMENT 'Account Number for the message',
  txa_origination_date_time STRING COMMENT 'It will have TXA Origination Date Time from MDM message OR OBR requested Date Time from ORU Message ',
  txa_transcription_date_time STRING COMMENT 'It will have TXA Transcription Date Time from MDM Message OR OBR Observation Date Time from ORU message ',
  msh_msg_type_message_code STRING COMMENT 'Message Type Code associated with the Health Level 7 (HL7) message.  i.e.: ADT, ORU, RAS, etc.',
  msh_msg_type_trigger_event STRING COMMENT 'Message Type Trigger Event Code associated with the Health Level 7 (HL7) message.  i.e.: A04, A05, etc.',
  pid_birth_date_time STRING COMMENT 'Patient Date of Birth',
  pid_pat_name_last_name STRING COMMENT 'Patient Last Name',
  pid_pat_name_first_name STRING COMMENT 'Patient First Name',
  pid_sex STRING COMMENT 'Patient Gender',
  pv1_adm_date_time STRING COMMENT 'The date and time the patient was admitted in the facility.',
  pv1_dchg_date_time STRING COMMENT 'The date and time the patient was discharged from the facility.',
  txa_originator_code_name_person_id STRING COMMENT 'This field contains the person identifier of the individual who originated (i.e., dictated) the document. The document originator may differ from the person responsible for authenticating the document.',
  txa_originator_code_name_last_name STRING COMMENT 'This field contains the last name of the individual who originated (i.e., dictated) the document.',
  txa_originator_code_name_first_name STRING COMMENT 'This field contains the first name of the individual who originated (i.e., dictated) the document.',
  txa_originator_code_name_middle_name STRING COMMENT 'This field contains the middle name of the individual who originated (i.e., dictated) the document.',
  txa_originator_code_name_suffix STRING COMMENT 'This field contains the name suffix of the individual who originated (i.e., dictated) the document.  i.e.: Junior, Senior, III, etc.',
  txa_originator_code_name_prefix STRING COMMENT 'This field contains the name salutaion of the individual who originated (i.e., dictated) the document.  i.e.: Mr, Mrs, Ms, etc.',
  txa_originator_code_name_degree STRING COMMENT 'This field contains the name degree (post nominal letters) of the individual who originated (i.e., dictated) the document.  i.e.: M.B.A, Ph.D, M.D., D.O., etc.',
  txa_filler_order_num STRING COMMENT 'This field is the order number associated with the filling application. Where a transcription service or similar organization creates the document and uses an internally unique identifier, that number should be inserted in this field',
  txa_document_type_primary_id STRING COMMENT 'This field contains the unique document type identifier for the type of document (as defined in the transcription system).',
  txa_document_type_text STRING COMMENT 'This field contains the description of the type of document (as defined in the transcription system).',
  txa_document_type_name_of_coding_sys STRING COMMENT 'Identifier for the coding system from which the values in the document type is obtained.',
  txa_document_completion_status STRING COMMENT 'This field identifies the current completion state of the document.  i.e.: Dictated, In Progress, Incomplete, etc.',
  obx_observation_value STRING COMMENT 'This field contains the value related to the OBX-3-observation identifier of the same segment. Depending upon the observation, the data type may be a number (e.g., respiratory rate), a coded answer (e.g., SNOMED), or a date/time a unit of blood is sent.',
  pid_pat_social_security_num STRING COMMENT 'The patient assigned Social Security Number used in the United States to track Social Security benefits and is also used for other identification purposes.',
  pid_pat_address_street_address1 STRING COMMENT 'The patient house number, apartment number, street name, street direction, P.O. Box number, and similar address information.',
  pid_pat_address_street_address2 STRING COMMENT 'This patient second line of address. In US usage, it qualifies address.  Examples: Suite 555 or Fourth Floor. When referencing an institution, this component specifies the street address.',
  pid_pat_address_city STRING COMMENT 'The city, or district or place where the patient is located (lives) depending upon the national convention for formatting addresses for postal usage.',
  pid_pat_address_state_province STRING COMMENT 'The state or province where the patient is located (lives). State or province should be represented by the official postal service codes for that country.',
  pid_pat_address_zip_postal_code STRING COMMENT 'The zip or postal code where the patient is located (lives). Zip or postal codes should be represented by the official codes for that country.',
  patient_type_status STRING,
  coid STRING,
  primary_care_physician STRING,
  pcp_phone STRING,
  message_created_date_time STRING,
  message_flag STRING,
  attending_physician STRING,
  etl_insert_date_time STRING COMMENT 'The date and time the record was inserted by the Extract Transform and Load (ETL) job.'
  )
PARTITIONED BY (
     message_type String,
	 transaction_date String
	 )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'='|','serialization.format'='|')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.SequenceFileInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat';

INSERT INTO cdm_scri.scri_patient_id_history_3 PARTITION(message_type,transaction_date)
SELECT
msh_msg_control_id as msh_msg_control_id,
msh_sending_facility as msh_sending_facility,
pid_medical_record_num as pid_medical_record_num,
pid_medical_record_urn as pid_medical_record_urn,
pid_pat_account_num as pid_pat_account_num,
txa_origination_date_time as txa_origination_date_time,
txa_transcription_date_time as txa_transcription_date_time,
msh_msg_type_message_code as msh_msg_type_message_code,
msh_msg_type_trigger_event as msh_msg_type_trigger_event,
pid_birth_date_time as pid_birth_date_time,
pid_pat_name_last_name as pid_pat_name_last_name,
pid_pat_name_first_name as pid_pat_name_first_name,
pid_sex as pid_sex,
pv1_adm_date_time as pv1_adm_date_time,
pv1_dchg_date_time as pv1_dchg_date_time,
txa_originator_code_name_person_id as txa_originator_code_name_person_id,
txa_originator_code_name_last_name as txa_originator_code_name_last_name,
txa_originator_code_name_first_name as txa_originator_code_name_first_name,
txa_originator_code_name_middle_name as txa_originator_code_name_middle_name,
txa_originator_code_name_suffix as txa_originator_code_name_suffix,
txa_originator_code_name_prefix as txa_originator_code_name_prefix,
txa_originator_code_name_degree as txa_originator_code_name_degree,
txa_filler_order_num as txa_filler_order_num,
txa_document_type_primary_id as txa_document_type_primary_id,
txa_document_type_text as txa_document_type_text,
txa_document_type_name_of_coding_sys as txa_document_type_name_of_coding_sys,
txa_document_completion_status as txa_document_completion_status,
obx_observation_value as obx_observation_value,
CASE WHEN pid_pat_social_security_num = '' THEN
( CASE WHEN split(pid_pat_id_list_identifier_type_code,"^")[1] = 'SS' THEN split(pid_pat_id_list_identifier_num,"^")[1] else '' END ) else
split_part(pid_pat_id_list_identifier_num,"^",2) else pid_pat_social_security_num END AS pid_pat_social_security_num,
pid_pat_address_street_address1 as pid_pat_address_street_address1,
pid_pat_address_street_address2 as pid_pat_address_street_address2,
pid_pat_address_city as pid_pat_address_city,
pid_pat_address_state_province as pid_pat_address_state_province,
pid_pat_address_zip_postal_code as pid_pat_address_zip_postal_code,
'' as patient_type_status,
'' as coid,
'' as primary_care_physician,
'' as pcp_phone,
'' as message_created_date_time,
'' as message_flag,
'' as attending_physician,
etl_insert_date_time as etl_insert_date_time,
message_type as message_type,
transaction_date as transaction_date
FROM cdm_scri.scri_patient_id_history ;





