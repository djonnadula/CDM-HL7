refresh hl7.hl7_audit_data; invalidate metadata hl7.hl7_audit_data;
refresh cdm_scri.nlp_patient_id_data; invalidate metadata cdm_scri.nlp_patient_id_data;

select 
a.process_name as RAW_MSG, 
b.process_name as JSON_MSG,  
upper(Substring(c.process_name,1, Instr(c.process_name,':')-1)) as SEG_MSG, 
substring(a.msg_create_date_time,1,8) as MSG_Create_Date, 
substring(a.etl_firstinsert_datetime,1,8) as Raw_ETL_Insert_Date, 
substring(b.etl_firstinsert_datetime,1,8) as JSON_ETL_Insert_Date, 
substring(c.etl_firstinsert_datetime,1,8) as SEGMENTS_ETL_Insert_Date,
count(*) as RowCount

from hl7.hl7_audit_data a

Full Outer Join hl7.hl7_audit_data b
on Trim(b.msg_control_id) = Trim(a.msg_control_id)
and upper(Trim(b.process_name)) like '%-JSON'
and Upper(Substring(a.process_name,1, Instr(a.process_name,'-')-1)) = upper(Substring(b.process_name,1, Instr(b.process_name,'-')-1))

Full Outer Join hl7.hl7_audit_data c
on Trim(c.msg_control_id) = Trim(a.msg_control_id)
and Upper(Trim(c.process_name)) like '%-SEGMENTS:%'
and Upper(Substring(a.process_name,1, Instr(a.process_name,'-')-1)) = upper(Substring(c.process_name,1, Instr(c.process_name,'-')-1))

Where Upper(Trim(a.process_name)) like '%-RAW'
and (a.msg_control_id is null or b.msg_control_id is null or c.msg_control_id is null)
and substring(a.etl_firstinsert_datetime,1,12) between
  from_unixtime(unix_timestamp(now() - interval 5 hours), 'yyyyMMddHHmm')
  and from_unixtime(unix_timestamp(now() - interval 1 hour), 'yyyyMMddHHmm')
group by 1, 2,3,4,5,6,7 order by 1 asc, 5 desc;
