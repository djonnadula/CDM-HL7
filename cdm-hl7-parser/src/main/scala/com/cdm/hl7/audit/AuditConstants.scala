package com.cdm.hl7.audit

/**
  * Created by Devaraj Jonnadula on 9/1/2016.
  *
  * Constants for Audits
  */
object AuditConstants {

  lazy val msg_control_id = "message_control_id"
  lazy val msg_create_time = "date_time_of_message"
  lazy val medical_record_num = "medical_record_num"
  lazy val medical_record_urn = "medical_record_urn"
  lazy val account_num = "patient_account_num"
  lazy val sending_facility = "sending_facility"
  lazy val msh_msg_type = "message_type"
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
  lazy val hl7RawIOFun = "hl7RawIO"
  lazy val hl7RawAuditIOFun = "hl7RawAuditIO"
  lazy val cdmScri = "CDMSCRI"
  lazy val processed = "PROCESSED"
  lazy val applicationSending = "S"
  lazy val applicationReceiving = "R"
  lazy val WSMQ = "WSMQ"
  lazy val HDFS = "HDFS"

}
