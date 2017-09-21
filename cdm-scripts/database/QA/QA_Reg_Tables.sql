use hl7_qa;


CREATE IF NOT EXISTS EXTERNAL TABLE hl7_json (
hl7 STRING
)
PARTITIONED BY (
	message_type String,
	transaction_date String
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS SEQUENCEFILE
LOCATION '/user/hive/warehouse/hl7_qa/landing_zone=JSON';



CREATE EXTERNAL TABLE scri_patid (
patid_data STRING
)
PARTITIONED BY (
	message_type String,
	transaction_date String
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS SEQUENCEFILE
LOCATION '/user/hive/warehouse/hl7_qa/landing_zone=ADHOC-SCRIPATID-JSON';



CREATE EXTERNAL TABLE cdi_nlp_data (
nlp_data STRING
)
PARTITIONED BY (
	message_type String,
	transaction_date String
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS SEQUENCEFILE
LOCATION '/user/hive/warehouse/hl7_qa/landing_zone=ADHOC-CDI-DELIMITED';


