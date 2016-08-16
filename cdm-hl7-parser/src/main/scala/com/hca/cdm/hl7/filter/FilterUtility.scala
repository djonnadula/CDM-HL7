package com.hca.cdm.hl7.filter

import com.hca.cdm.utils.DateUtil.{currentTimeStamp => timeStamp}
import com.hca.cdm.hl7.constants.HL7Types.{withName => hl7, _}

import scala.collection.mutable
import com.hca.cdm.hl7.constants.HL7Constants._
import scala.annotation.tailrec

/**
  * Created by Devaraj Jonnadula on 8/10/2016.
  */
class FilterUtility {

  private lazy val EMPTYSTR = ""
  private type defaultType = mutable.LinkedHashMap[String, Any]
  private type listType = mutable.MutableList[mutable.LinkedHashMap[String, Any]]

  def transformToDelim(reqMsgType: HL7, delimiter: String, timeStampReq: Boolean = true)(filterKeys: Map[String, String], data: defaultType): Option[String] = {
    if (filterKeys.isEmpty | !isRequiredType(data, reqMsgType)) return None
    val observationValue = new mutable.MutableList[String]
    data.foldLeft(new StringBuilder(filterKeys.size * 30, EMPTYSTR))((builder, node) => filterData(builder, delimiter, observationValue)(filterKeys, node._2.asInstanceOf[defaultType])) match {
      case out => if (out.nonEmpty) {
        observationValue.addString(out)
        out ++= delimiter
        if (timeStampReq) includeEle(out, delimiter, timeStamp)
        Some(out.result)
      }
      else None
      case _ => None
    }
  }

  @tailrec private def filterData(underlying: StringBuilder, delimiter: String, observationCol: mutable.MutableList[String])(filterKeys: Map[String, String], data: defaultType): StringBuilder = {
    data.headOption match {
      case Some(node) =>
        node._1 != null & node._1 != EMPTYSTR & (filterKeys contains node._1) match {
          case true => node._2 match {
            case str: String =>
              if (node._1 == Observation_Col) observationCol += str
              else includeEle(underlying, delimiter, node._2.asInstanceOf[String])
              filterData(underlying, delimiter, observationCol)(filterKeys, data.tail)
            case mapType: defaultType => filterData(underlying, delimiter, observationCol)(filterKeys, mapType)
            case listType: listType =>
              val tempData = new mutable.LinkedHashMap[String, Any]()
              listType.foreach(map => map.foreach(e => tempData += e))
              tempData ++= data.tail
              filterData(underlying, delimiter, observationCol)(filterKeys, tempData)
            case any: Any => throw new RuntimeException("Got an Unhandled Type into Filter Util ::  " + any.getClass + " ?? Not Yet Implemented")
          }
          case _ => filterData(underlying, delimiter, observationCol)(filterKeys, data.tail)
        }

      case _ => underlying

    }
  }

  private def includeEle(underlying: mutable.StringBuilder, delim: String, key: String) = {
    underlying ++= key
    underlying ++= delim
  }

  private def isRequiredType(data: defaultType, reqMsgType: HL7): Boolean = {
    data.get(MSH_Segment) match {
      case Some(nextSeg) => nextSeg.asInstanceOf[defaultType].get(Message_Type_Segment) match {
        case Some(seg) => seg.asInstanceOf[defaultType].get(Message_Control_Id) match {
          case Some(typ) => if (typ == classOf[String]) return hl7(typ.asInstanceOf[String]) == reqMsgType
          case _ =>
        }

      }
      case _ =>
    }
    false
  }
}
