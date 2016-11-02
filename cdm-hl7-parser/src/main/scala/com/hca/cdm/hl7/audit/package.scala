package com.hca.cdm.hl7

import com.hca.cdm._
import com.hca.cdm.hl7.audit.AuditConstants._
import com.hca.cdm.hl7.constants.HL7Constants._
import com.hca.cdm.hl7.model._
import com.hca.cdm.utils.DateUtil.{currentTimeStamp => timeStamp}
import scala.collection.mutable

/**
  * Created by Devaraj Jonnadula on 9/8/2016.
  */
package object audit {


  case class MSGMeta(controlId: String, msgCreateTime: String, medical_record_num: String, medical_record_urn: String, account_num: String, facility: String = EMPTYSTR)

  private lazy val NONE = MSGMeta(EMPTYSTR, EMPTYSTR, EMPTYSTR, EMPTYSTR, EMPTYSTR)

  def msgMeta(data: mutable.LinkedHashMap[String, Any]): MSGMeta = {
    (data.get(MSH_INDEX), data.get(commonNodeStr)) match {
      case (Some(msh), Some(common)) =>
        (msh, common) match {
          case (mshMap: mapType, cmnMap: mapType) => (cmnMap get msg_control_id, mshMap get msg_create_time,
            cmnMap get medical_record_num, cmnMap get medical_record_urn, cmnMap get account_num, cmnMap get sending_facility) match {
            case (Some(control: String), Some(createTime: String), Some(rnum: String), Some(urn: String), Some(accnum: String), Some(facility: String)) =>
              return MSGMeta(control, createTime, rnum, urn, accnum, facility)
            case _ =>
          }
          case _ =>
        }
      case _ =>
    }
    NONE
  }

  def segmentsInMsg(segments: Set[String], data: mutable.LinkedHashMap[String, Any]): String = {
    data.map({ case (k, v) =>
      val seg = k.substring(k.indexOf(".") + 1)
      if (segments contains seg) seg
      else EMPTYSTR
    }).filter(_ != EMPTYSTR).toSet mkString repeat
  }


  def auditMsg(hl7Str: String, stage: String)(segments: String = EMPTYSTR, meta: MSGMeta): String = {
    segments match {
      case EMPTYSTR =>
        s"$hl7Str-$stage$PIPE_DELIMITED${meta.controlId}$PIPE_DELIMITED${meta.msgCreateTime}" +
            s"$PIPE_DELIMITED${meta.medical_record_num}$PIPE_DELIMITED${meta.medical_record_urn}" +
            s"$PIPE_DELIMITED${meta.account_num}$PIPE_DELIMITED$timeStamp"
      case _ =>
        s"$hl7Str-$stage$COLON$segments$PIPE_DELIMITED${meta.controlId}$PIPE_DELIMITED${meta.msgCreateTime}" +
            s"$PIPE_DELIMITED${meta.medical_record_num}$PIPE_DELIMITED${meta.medical_record_urn}$PIPE_DELIMITED" +
            s"${meta.account_num}$PIPE_DELIMITED$timeStamp"
    }
  }

  def header(hl7Str: String, stage: String, meta: Either[MSGMeta, String]): String = {
    meta match {
      case Left(x) => s"$hl7Str$COLON$stage$COLON${x.msgCreateTime}"
      case Right(y) => s"$hl7Str$COLON$stage$COLON${metaFromRaw(y).msgCreateTime}"
    }

  }


}
