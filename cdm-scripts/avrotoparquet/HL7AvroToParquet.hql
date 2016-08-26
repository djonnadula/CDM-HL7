-- Directory config
SET hive.input.dir.recursive=true;
SET hive.mapred.supports.subdirectories=true;
SET hive.supports.subdirectories=true;
SET mapred.input.dir.recursive=true;

-- Compression config
SET hive.exec.compress.output=true;
SET parquet.compression=SNAPPY;

-- Partitioning config
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=10000000;
SET hive.exec.max.dynamic.partitions.pernode=10000000;

-- Java memory config
-- Map container heap size
SET mapreduce.map.memory.mb=8192;
-- Map JVM heap size (0.8*map container memory allocation (8192))
SET mapreduce.map.java.opts=-Xmx6553m;
-- Reduce container heap size
SET mapreduce.reduce.memory.mb=4096;
-- Reduce JVM heap size (0.8*reduce container memory allocation (4096))
SET mapreduce.reduce.java.opts=-Xmx3276m;

-- Inserting all data from avro table into hl7_parquet_table partitioned by day
INSERT INTO TABLE hl7.hl7_parquet_data PARTITION(message_date)
SELECT DISTINCT
    parse_result,
    message_control_id,
    message_version,
    message_type_trigger,
    message_segments,
    message_time,
    dbmid,
    dbm_message_time,
    patient_name,
    patient_alt_id,
    patient_dob,
    obx_observation_text,
    obx_text_length,
    rxe_admin_instructions,
    nte_comments,
    generic_fields,
    raw_record,
    current_timestamp AS firstinsertdatetime,
    SUBSTR(message_type_trigger, 1, 3) AS message_type,
    source_facility,
    FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING(message_time, 1, 8), 'yyyyMMdd'), 'yyyy-MM-dd') AS message_date
FROM hl7.hl7_avro_data_today
;


