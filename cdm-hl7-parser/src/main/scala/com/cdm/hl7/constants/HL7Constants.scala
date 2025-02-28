package com.cdm.hl7.constants

import scala.collection.mutable
import scala.util.matching.Regex

/**
  * Created by Devaraj Jonnadula on 8/10/2016.
  *
  * Most Commonly Used Constants for HL7
  */
object HL7Constants {

  lazy val COMN = "COMN"
  lazy val MERGE = "merge"
  lazy val MOVE = "move"
  lazy val MSH = "MSH"
  lazy val PID = "PID"
  lazy val GT1 = "GT1"
  lazy val PRIMARY_IN1 = "IN1|1|"
  lazy val IN1 = "IN1"
  lazy val PV1 = "PV1"
  lazy val RAW = "RAW"
  lazy val REPTN_DELIM = "reptn_delim"
  lazy val CMPNT_DELIM = "cmpnt_delim"
  lazy val SUBCMPNT_DELIM = "subcmpnt_delim"
  lazy val TRUNC_DELIM = "trunc_delim"
  lazy val ESC_DELIM = "esc_delim"
  lazy val FIELD_DELIM = "field_delim"
  lazy val MT_ = "MT_"
  lazy val MT6_ = "MT6_"
  lazy val EPIC_ = "EPIC_"
  lazy val ECW_ = "ECW_"
  lazy val NG_ = "NG_"
  lazy val IP_ = "IP_"
  lazy val ZEROStr = "0"
  lazy val REPEAT_ZERO_STAR = "^0*"
  lazy val NO_COLUMN_ASSIGNED = "no_column_assigned"
  lazy val HL7_2_1 = "2.1"
  lazy val HL7_2_2 = "2.2"
  lazy val HL7_2_3 = "2.3"
  lazy val HL7_2_3_1 = "2.3.1"
  lazy val HL7_2_4 = "2.4"
  lazy val HL7_2_5 = "2.5"
  lazy val HL7_2_5_1 = "2.5.1"
  lazy val PIPER: Regex = "\\|".r
  lazy val COMMAR: Regex = ",".r
  lazy val COMMA = ","
  lazy val SEMICOLUMN = ";"
  lazy val PIPE_DELIMITED_STR = "|"
  lazy val COLON = ":"
  lazy val UNKNOWN = "unknown"
  lazy val underScore = "_"
  type mapType = mutable.LinkedHashMap[String, Any]
  type listType = mutable.ListBuffer[mutable.LinkedHashMap[String, Any]]


}
