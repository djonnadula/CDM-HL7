package com.hca.cdm.hl7

import com.hca.cdm.Models.MSGMeta
import com.hca.cdm._
import com.hca.cdm.hl7.audit.AuditConstants._
import com.hca.cdm.hl7.constants.HL7Constants._
import com.hca.cdm.hl7.model._
import com.hca.cdm.utils.DateUtil.{currentTimeStamp => timeStamp}
import scala.collection.mutable
import scala.language.postfixOps

/**
  * Created by Devaraj Jonnadula on 9/8/2016.
  */
package object audit {


  private lazy val NONE = MSGMeta(EMPTYSTR, EMPTYSTR, EMPTYSTR, EMPTYSTR, EMPTYSTR)

  def msgMeta(data: mutable.LinkedHashMap[String, Any]): MSGMeta = {
    (data.get(MSH_INDEX), data.get(commonNodeStr)) match {
      case (Some(msh), Some(common)) =>
        (msh, common) match {
          case (mshMap: mapType, cmnMap: mapType) => (cmnMap get msg_control_id, mshMap get msg_create_time,
            cmnMap get medical_record_num, cmnMap get medical_record_urn, cmnMap get account_num, cmnMap get sending_facility) match {
            case (Some(control: String), Some(createTime: String), Some(rnum: String), Some(urn: String), Some(accnum: String), Some(facility: String)) =>
              return MSGMeta(control, createTime, rnum, urn, accnum, facility, triggerEvent(msh))
            case _ =>
          }
          case _ =>
        }
      case _ =>
    }
    NONE
  }

  private def triggerEvent(msh: Any): String = {
    msh match {
      case map: mapType =>
        if (map isDefinedAt msh_msg_type) {
          val typ = map(msh_msg_type).asInstanceOf[mapType]
          if (typ isDefinedAt trigger_event) return typ(trigger_event).asInstanceOf[String]
        }
      case _ =>
    }
    EMPTYSTR
  }

  def segmentsInMsg(segments: Set[String], data: mutable.LinkedHashMap[String, Any]): String = {
    data.map({ case (k, v) =>
      val seg = k.substring(k.indexOf(DOT) + 1)
      if (segments contains seg) seg
      else EMPTYSTR
    }).filter(_ != EMPTYSTR).toSet mkString caret
  }


  def auditMsg(hl7Str: String, stage: String)(segments: String = EMPTYSTR, meta: MSGMeta): String = {
    segments match {
      case EMPTYSTR =>
        s"$hl7Str-$stage$PIPE_DELIMITED${meta.controlId}$PIPE_DELIMITED${meta.msgCreateTime}$PIPE_DELIMITED${meta.medical_record_num}$PIPE_DELIMITED${meta.medical_record_urn}$PIPE_DELIMITED${meta.account_num}$PIPE_DELIMITED$timeStamp"
      case _ =>
        s"$hl7Str-$stage$COLON$segments$PIPE_DELIMITED${meta.controlId}$PIPE_DELIMITED${meta.msgCreateTime}$PIPE_DELIMITED${meta.medical_record_num}$PIPE_DELIMITED${meta.medical_record_urn}$PIPE_DELIMITED${meta.account_num}$PIPE_DELIMITED$timeStamp"
    }
  }

  def tlmAckMsg(hl7: String, ackingTo: String = "WSMQ", from: String = "HDFS")(meta: MSGMeta): String = {
    s"$ackingTo$PIPE_DELIMITED${meta.facility}$PIPE_DELIMITED$hl7$PIPE_DELIMITED${meta.controlId}$PIPE_DELIMITED${meta.msgCreateTime}$PIPE_DELIMITED${meta.triggerEvent}$PIPE_DELIMITED$from$PIPE_DELIMITED$timeStamp"
  }


  def header(hl7Str: String, stage: String, meta: Either[MSGMeta, String]): String = {
    meta match {
      case Left(x) => s"$hl7Str$COLON$stage$COLON${x.msgCreateTime}"
      case Right(y) => s"$hl7Str$COLON$stage$COLON${metaFromRaw(y).msgCreateTime}"
    }

  }


}
