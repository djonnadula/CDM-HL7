package com.hca.cdm.hl7.model

import com.hca.cdm._
import com.hca.cdm.hl7.constants.HL7Constants._
import com.hca.cdm.hl7.constants.HL7Types.{withName => hl7, _}
import com.hca.cdm.log.Logg
import com.hca.cdm.utils.DateUtil.{currentTimeStamp => timeStamp}

import scala.collection.mutable

/**
  * Created by Devaraj Jonnadula on 8/18/2016.
  */
private[model] class DataModeler(private val reqMsgType: HL7, private val timeStampReq: Boolean = true, private val outDelim: String = "|")
  extends Logg {
  logIdent = "for HL7 Type :: " + reqMsgType.toString
  private lazy val notValid = Hl7SegmentTrans(Right(Some(notValidStr), null))
  private lazy val skipped = Hl7SegmentTrans(Right(Some(skippedStr), null))
  scala.collection.immutable.::

  def applyModel(whichSeg: String, model: Model)(data: mapType): Hl7SegmentTrans = {
    try {
      val modelFilter: Map[String, mutable.Set[String]] = model.modelFilter
      if (modelFilter.isEmpty | !isRequiredType(data, reqMsgType)) return notValid
      val layout = model.layoutCopy
      val appendData = includeEle(layout, _: String, _: String)(outDelim)
      var dataExist = false
      data.foldLeft(dataExist)((builder, node) => {
        node._1.substring(node._1.indexOf(".") + 1) == model.reqSeg match {
          case true =>
            if (modelData(layout, model)(modelFilter, node._2.asInstanceOf[mapType])(appendData)) dataExist = true
          case _ =>
        }
        dataExist
      }) match {
        case true => if (timeStampReq) layout += outDelim -> timeStamp
          handleCommonSegments(data, layout)
          Hl7SegmentTrans(Left(layout.valuesIterator mkString outDelim))
        case _ => skipped
      }

    } catch {
      case t: Throwable => Hl7SegmentTrans(Right(None, t))
    }
  }

  private def handleCommonSegments(data: mapType, layout: mutable.LinkedHashMap[String, String]) = {
    if (data.get(MSH_INDEX) isDefined) handleIndexes(MSH_INDEX, data, layout)
    if (data.get(PID_INDEX) isDefined) handleIndexes(PID_INDEX, data, layout)

  }

  private def handleIndexes(segIndex: String, data: mapType, layout: mutable.LinkedHashMap[String, String]) = {
    data.get(segIndex) match {
      case Some(node) => commonNode foreach (ele => {
        node match {
          case map: mapType => map.get(ele._1) match {
            case Some(v: String) => layout update(ele._1, v)
            case Some(m: mapType) => m.get(ele._1) match {
              case Some(mv: String) => layout update(ele._1, mv)
              case _ =>
            }
            case _ =>
          }
          case _ =>
        }

      })
    }
    commonNode.foreach({ case (k, v) =>
      (layout isDefinedAt k) & (data isDefinedAt k) match {
        case true => layout update(k, data(k).asInstanceOf[String])
        case _ =>
      }

    })

  }

  private def includeEle(underlying: mutable.LinkedHashMap[String, String], key: String, req: String)(delimiter: String) = {
    underlying get key match {
      case Some(x) => x match {
        case EMPTYSTR => underlying update(key, req)
        case exists => underlying update(key, exists ++ req)
      }
      case k => throw new DataModelException(key + " Key Not Found while Applying Model to Data :: " + k)
    }

  }

  private def modelData(underlying: mutable.LinkedHashMap[String, String], model: Model)(filterKeys: Map[String, mutable.Set[String]], data: mapType)(appendData: (String, String) => Unit): Boolean = {
    var dataExist = false
    underlying update(segmentkey, model.reqSeg)
    data.foreach(node => {
      node._1 != null & node._1 != EMPTYSTR & (filterKeys isDefinedAt node._1) match {
        case true => node._2 match {
          case str: String => if (underlying isDefinedAt node._1) {
            appendData(node._1, str)
            dataExist = true
          }
          case map: mapType => handelMap(map, node._1, filterKeys, model)(underlying, appendData)
          case list: listType => handleList(list, node._1, filterKeys, model)(underlying, appendData)
          case any: Any => throw new DataModelException("Got an Unhandled Type into Filter Util ::  " + any.getClass + " ?? Not Yet Implemented")
        }
        case _ =>
      }
    })
    dataExist
  }


  private def handelMap(data: mapType, node: String, filterKeys: Map[String, mutable.Set[String]], model: Model)
                       (underlying: mutable.LinkedHashMap[String, String], appendData: (String, String) => Unit): Unit = {
    data.foreach({
      case (k, v) =>
        v match {
          case str: String =>
            if (underlying isDefinedAt (node + model.modelFieldDelim + k)) appendData(node + model.modelFieldDelim + k, str)
            else if (underlying isDefinedAt node) appendData(node, str)
          case listType: listType => handleList(listType, node + model.modelFieldDelim + k, filterKeys, model)(underlying, appendData)
          case map: mapType => handelMap(map, node + model.modelFieldDelim + k, filterKeys, model)(underlying, appendData)
        }
    })
  }

  private def handleList(data: listType, node: String, filterKeys: Map[String, mutable.Set[String]], model: Model)
                        (underlying: mutable.LinkedHashMap[String, String], appendData: (String, String) => Unit): Unit = {
    data.foreach(map => map.foreach({ case (k, v) =>
      v match {
        case str: String =>
          if (underlying isDefinedAt (node + model.modelFieldDelim + k)) appendData(node + model.modelFieldDelim + k, str)
          else if (underlying isDefinedAt node) appendData(node, str)
        case list: listType => handleList(list, node + model.modelFieldDelim + k, filterKeys, model)(underlying, appendData)
        case map: mapType => handelMap(map, node + model.modelFieldDelim + k, filterKeys, model)(underlying, appendData)
      }
    }))
  }

  private def isRequiredType(data: mapType, reqMsgType: HL7): Boolean = {
    data.get(MSH_Segment) match {
      case Some(nextSeg) => nextSeg.asInstanceOf[mapType].get(Message_Type_Segment) match {
        case Some(seg) => seg.asInstanceOf[mapType].get(Message_Control_Id) match {
          case Some(typ) => typ match {
            case s: String => return hl7(s) == reqMsgType
            case _ =>
          }
          case _ =>
        }
        case _ =>
      }
      case _ =>
    }
    false
  }


}

object DataModeler {
  def apply(reqMsgType: HL7, timeStampReq: Boolean = true, outDelim: String = "|"): DataModeler =
    new DataModeler(reqMsgType, timeStampReq, outDelim)

  def apply(reqMsgType: HL7): DataModeler = new DataModeler(reqMsgType, timeStampReq = true, outDelim = "|")

}
