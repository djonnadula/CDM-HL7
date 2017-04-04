package com.hca.cdm.hl7.audit

/**
  * Created by Devaraj Jonnadula on 9/1/2016.
  *
  * Constants for Audits
  */
object AuditConstants {

  lazy val msg_control_id = "msh_msg_control_id"
  lazy val msg_create_time = "msh_msg_create_date_time"
  lazy val medical_record_num = "pid_medical_record_num"
  lazy val medical_record_urn = "pid_medical_record_urn"
  lazy val account_num = "pid_pat_account_num"
  lazy val sending_facility = "msh_sending_facility"
  lazy val msh_msg_type = "msh_msg_type"
  lazy val trigger_event = "trigger_event"
  lazy val rawStage = "RAW"
  lazy val jsonStage = "JSON"
  lazy val segmentStage = "SEG"
  lazy val segmentsInHL7 = "SEGMENTS"
  lazy val adhocStage = "ADHOC"
  lazy val rejectStage = "REJECT"
  lazy val auditHeader = "AUDIT"
  lazy val missingTemplate = "TEMPLATE_MAPPING"
  lazy val invalidHl7 = "Not a Valid HL7. Req Info Not Found. Check Message Requirement"
  lazy val hl7JsonIOFun = "hl7JsonIO"
  lazy val hl7JsonAuditIOFun = "hl7JsonAuditIO"
  lazy val hl7RawIOFun = "hl7JsonIO"
  lazy val hl7RawAuditIOFun = "hl7JsonAuditIO"
  lazy val cdmScri = "CDMSCRI"
  lazy val processed = "PROCESSED"
  lazy val applicationSending = "S"
  lazy val applicationReceiving = "R"
  lazy val WSMQ = "WSMQ"
  lazy val HDFS = "HDFS"

}
