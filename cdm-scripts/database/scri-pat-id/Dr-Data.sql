use cdm_scri;

drop table cdm_synthesys_patientid_results;

CREATE EXTERNAL TABLE cdm_synthesys_patientid_results (
 unique_message_id STRING,
   facility_mnemonic STRING,
   patient_account_number STRING,
   patient_mrn STRING,
   patient_urn STRING,
   patient_last_name STRING,
   patient_first_name STRING,
   patient_dob STRING,
   patient_gender STRING,
   message_origin_or_requested_date_time STRING,
   message_signed_or_observation_date_time STRING,
   ingestion_date_time STRING,
   model_predicted_primary_site STRING,
   model_predicted_primary_site_icdo3 STRING,
   suggested_primary_site STRING,
   suggested_primary_site_icdo3 STRING,
   submitted_primary_site STRING,
   submitted_primary_site_icdo3 STRING,
   transition_of_care STRING,
   user_action STRING,
   user_action_date_time STRING,
   report_assigned_to_user_at STRING,
   user STRING,
   site_and_associated_model_output_score STRING,
   message_type STRING,
   message_event_type STRING,
   document_type_identifier STRING,
   document_type_text STRING,
   document_type_name_of_coding_sys STRING,
   social_security_number STRING,
   patient_address_1 STRING,
   patient_address_2 STRING,
   patient_city STRING,
   patient_state STRING,
   patient_zip_code STRING,
   patient_type_status STRING,
   coid STRING,
   primary_care_physician STRING,
   pcp_phone STRING,
   message_created_datetime STRING,
   message_flag STRING,
   etl_firstinsert_datetime STRING,
   attending_physician STRING,
   meditech_network_id STRING,
   user_action_criticality STRING,
   message_text STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS SEQUENCEFILE
LOCATION '/user/hive/warehouse/cdm_scri/cdm_synthesys_patientid_results/landing_zone=valid';


CREATE EXTERNAL TABLE cdm_synthesys_patientid_results_rejected (
 msg STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS SEQUENCEFILE
LOCATION '/user/hive/warehouse/cdm_scri/cdm_synthesys_patientid_results/landing_zone=rejected';