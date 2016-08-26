package com.hca.cdm.hl7.constants

import scala.collection.mutable

/**
  * Created by Devaraj Jonnadula on 8/10/2016.
  */
object HL7Constants {

  lazy val MERGE = "merge"
  lazy val MOVE = "move"
  lazy val MSH = "MSH"
  lazy val REPTN_DELIM = "reptn_delim"
  lazy val CMPNT_DELIM = "cmpnt_delim"
  lazy val SUBCMPNT_DELIM = "subcmpnt_delim"
  lazy val TRUNC_DELIM = "trunc_delim"
  lazy val ESC_DELIM = "esc_delim"
  lazy val FIELD_DELIM = "field_delim"
  lazy val MT_ = "MT_"
  lazy val MT6_ = "MT6_"
  lazy val EPIC_ = "^EPIC_"
  lazy val ECW_ = "ECW_"
  lazy val NG_ = "NG_"
  lazy val IP_ = "IP_"
  lazy val ZEROStr = "0"
  lazy val REPEAT_ZERO_STAR = "^0*"
  lazy val NO_COLUMN_ASSIGNED = "no_column_assigned"
  lazy val HL7_2_1 = "2.1"
  lazy val HL7_2_5_1 = "2.5.1"
  lazy val HL7_2_4 = "2.4"
  lazy val MSH_Segment = "0001.MSH"
  lazy val Message_Type_Segment = "009.msh_msg_type"
  lazy val Message_Control_Id = "001.message_code"
  lazy val Msg_Type_Hier = Seq(MSH_Segment, Message_Type_Segment, Message_Control_Id)
  lazy val Observation_Col = "005.obx_observation_value"
  lazy val PIPER = "\\|".r
  lazy val COMMAR = ",".r
  lazy val PIPE_NES = "|"
  type mapType = mutable.LinkedHashMap[String, Any]
  type listType = mutable.ListBuffer[mutable.LinkedHashMap[String, Any]]


}
