package com.hca.cdm.hl7.audit

/**
  * Created by Devaraj Jonnadula on 9/1/2016.
  *
  * Constants for Audits
  */
object AuditConstants {

  lazy val msg_control_id = "010.msh_msg_control_id"
  lazy val msg_create_time = "007.msh_msg_create_date_time"
  lazy val medical_record_num = "003.pid_medical_record_num"
  lazy val medical_record_urn = "004.pid_medical_record_urn"
  lazy val account_num = "018.pid_pat_account_num"
  lazy val sending_facility = "004.msh_sending_facility"
  lazy val rawStage = "RAW"
  lazy val jsonStage = "JSON"
  lazy val segmentStage = "SEG"
  lazy val segmentsInHL7 = "SEGMENTS"
  lazy val adhocStage = "ADHOC"
  lazy val rejectStage = "REJECT"
  lazy val invalidHl7 = "Not a Valid HL7. Req Info Not Found. Check Message for Requirement "
  lazy val hl7JsonIOFun ="hl7JsonIO"
  lazy val hl7JsonAuditIOFun = "hl7JsonAuditIO"


}
