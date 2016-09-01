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
  private lazy val skipped = Right(Some(skippedStr), null)
  private lazy val notValid = List(Hl7SegmentTrans(Right(Some(notValidStr), null)))


  def applyModel(whichSeg: String, model: Model)(data: mapType): Traversable[Hl7SegmentTrans] = {
    val modelFilter: Map[String, mutable.Set[String]] = model.modelFilter
    if (modelFilter.isEmpty | !isRequiredType(data, reqMsgType)) return notValid
    var layout: mutable.LinkedHashMap[String, String] = model.EMPTY
    val appendData = includeEle(layout, _: String, _: String, _: String)(outDelim)
    data.map(node => {
      try {
        node._1.substring(node._1.indexOf(".") + 1) == model.reqSeg match {
          case true => layout = model.layoutCopy
            modelData(layout, model)(modelFilter, node._2.asInstanceOf[mapType])(appendData) match {
              case true =>
                handleCommonSegments(data, layout)
                val builder = new StringBuilder(layout.size * 40)
                layout.foreach({ case (k, v) => builder append (v + outDelim) })
                if (timeStampReq) builder append timeStamp
                Left(builder.toString())
              case _ => skipped
            }
          case _ => skipped
        }
      }
      catch {
        case t: Throwable => Right(None, t)
      }
    }) map (Hl7SegmentTrans(_))
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
            case Some(m: mapType) => m.foreach({ case (mk, mv) =>
              if (mk.substring(mk.indexOf(".") + 1) == ele._1.substring(ele._1.indexOf(".") + 1)) {
                mv match {
                  case str: String => layout update(ele._1, str)
                  case _ =>
                }
              }
            })

            case _ =>
          }
          case _ =>
        }

      })
    }

  }

  private def includeEle(underlying: mutable.LinkedHashMap[String, String], key: String, req: String, repeat: String = EMPTYSTR)(delimiter: String): Unit = {
    if (req == EMPTYSTR & repeat == EMPTYSTR) return
    underlying get key match {
      case Some(x) => x match {
        case EMPTYSTR => underlying update(key, req)
        case exists => if (repeat == EMPTYSTR) underlying update(key, exists ++ req)
        else underlying update(key, exists ++ repeat ++ req)
      }
      case k => throw new DataModelException(key + " Key Not Found while Applying Model to Data :: " + k)
    }

  }

  private def modelData(underlying: mutable.LinkedHashMap[String, String], model: Model)(filterKeys: Map[String, mutable.Set[String]], data: mapType)
                       (appendData: (String, String, String) => Unit): Boolean = {
    var dataExist = false
    data.foreach(node => {
      node._1 != null & node._1 != EMPTYSTR & (filterKeys isDefinedAt node._1) match {
        case true => node._2 match {
          case str: String => if (underlying isDefinedAt node._1) {
            appendData(node._1, str, EMPTYSTR)
            dataExist = true
          }
          case map: mapType => handelMap(map, node._1, model)(filterKeys)(underlying, appendData)
          case list: listType => handleList(list, node._1, model)(filterKeys)(underlying, appendData)
          case any: Any => throw new DataModelException("Got an Unhandled Type into Filter Util ::  " + any.getClass + " ?? Not Yet Implemented")
        }
        case _ =>
      }
    })
    dataExist
  }


  private def handelMap(data: mapType, node: String, model: Model)(filterKeys: Map[String, mutable.Set[String]])
                       (underlying: mutable.LinkedHashMap[String, String], appendData: (String, String, String) => Unit): Unit = {
    data.foreach({
      case (k, v) =>
        v match {
          case str: String =>
            if (underlying isDefinedAt (node + model.modelFieldDelim + k)) appendData(node + model.modelFieldDelim + k, str, EMPTYSTR)
            else if ((underlying isDefinedAt node) & !isEmpty(node, filterKeys)) appendData(node, str, EMPTYSTR)
          case listType: listType => handleList(listType, node + model.modelFieldDelim + k, model)(filterKeys)(underlying, appendData)
          case map: mapType => handelMap(map, node + model.modelFieldDelim + k, model)(filterKeys)(underlying, appendData)
        }
    })
  }

  private def handleList(data: listType, node: String, model: Model)(filterKeys: Map[String, mutable.Set[String]])
                        (underlying: mutable.LinkedHashMap[String, String], appendData: (String, String, String) => Unit): Unit = {
    data.foreach(map => map.foreach({ case (k, v) =>
      v match {
        case str: String =>
          if (underlying isDefinedAt (node + model.modelFieldDelim + k)) appendData(node + model.modelFieldDelim + k, str, repeat)
          else if ((underlying isDefinedAt node) & !isEmpty(node, filterKeys)) appendData(node, str, repeat)
        case list: listType => handleList(list, node + model.modelFieldDelim + k, model)(filterKeys)(underlying, appendData)
        case map: mapType => handelMap(map, node + model.modelFieldDelim + k, model)(filterKeys)(underlying, appendData)
      }
    }))
  }

  private def isEmpty(key: String, filterKeys: Map[String, mutable.Set[String]]): Boolean = {
    filterKeys get key match {
      case Some(x) => if (x ne null) return x.size > 1
      case _ =>
    }
    false
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
