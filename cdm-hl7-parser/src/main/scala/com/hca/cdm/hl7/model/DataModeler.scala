package com.hca.cdm.hl7.model

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.hca.cdm._
import com.hca.cdm.hl7.constants.HL7Constants._
import com.hca.cdm.hl7.constants.HL7Types.{withName => hl7, _}
import com.hca.cdm.hl7.model.OutFormats._
import com.hca.cdm.log.Logg
import com.hca.cdm.utils.DateUtil.{currentTimeStamp => timeStamp}

import scala.collection.mutable

/**
  * Created by Devaraj Jonnadula on 8/18/2016.
  */
private[model] class DataModeler(private val reqMsgType: HL7, private val timeStampReq: Boolean = true, private val outDelim: String = "|") extends Logg {
  logIdent = "for HL7 Type :: " + reqMsgType.toString
  private lazy val skipped = Hl7SegmentTrans(Right(skippedStr))
  private lazy val notValid = Hl7SegmentTrans(Right(notValidStr))
  private lazy val segmentDoesntExist = Hl7SegmentTrans(Right(NA))
  private lazy val NONE = new mutable.LinkedHashMap[String, Throwable] += (EMPTYSTR -> null)
  private lazy val toJson = new ObjectMapper().registerModule(DefaultScalaModule).writer.writeValueAsString(_)

  def applyModel(whichSeg: String, model: Model)(data: mapType): Hl7SegmentTrans = {
    val modelFilter: Map[String, mutable.Set[String]] = model.modelFilter
    if (modelFilter.isEmpty | !isRequiredType(data, reqMsgType)) return notValid
    var layout = model.EMPTY
    val dataHandler = includeEle(layout, _: String, _: String, _: String)
    val temp = model.adhoc match {
      case Some(adhoc) => layout = model.layoutCopy
        nodesTraversal(data, model, layout, modelFilter, dataHandler) match {
          case out => if (out._1) {
            adhoc.outFormat match {
              case JSON => out._2 += (toJson(model.adhocLayout(layout, adhoc.outKeyNames)) -> null)
              case DELIMITED => handleCommonSegments(data, layout)
                out._2 += (makeFinal(layout) -> null)
            }
          }
            out._2
        }
      case None => model.reqSeg match {
        case OBX_SEG => layout = model.layoutCopy
          nodesTraversal(data, model, layout, modelFilter, dataHandler, allNodes = false, OBX_SEG) match {
            case out => if (out._1) {
              handleCommonSegments(data, layout)
              out._2 += (makeFinal(layout) -> null)
            }
              out._2
          }
        case _ => data.map(node => {
          try {
            node._1.substring(node._1.indexOf(".") + 1) == model.reqSeg match {
              case true => layout = model.layoutCopy
                modelData(layout, model)(modelFilter, node._2.asInstanceOf[mapType])(dataHandler) match {
                  case true => handleCommonSegments(data, layout)
                    (makeFinal(layout), null)
                  case _ => (skippedStr, null)
                }
              case _ => (EMPTYSTR, null)
            }
          }
          catch {
            case t: Throwable => (null, t)
          }
        })
      }
    }
    temp filter (_._1 != EMPTYSTR) match {
      case out => if (valid(out)) Hl7SegmentTrans(Left(out))
      else segmentDoesntExist
    }
  }

  private def nodesTraversal(data: mapType, model: Model, layout: mutable.LinkedHashMap[String, String], modelFilter: Map[String, mutable.Set[String]],
                             dataHandler: (String, String, String) => Unit, allNodes: Boolean = true, whichSeg: String = EMPTYSTR) = {
    var dataExist = false
    val temp = data.map(node => {
      try {
        allNodes match {
          case true => if (node._1 != commonNodeStr) if (modelData(layout, model)(modelFilter, node._2.asInstanceOf[mapType])(dataHandler, appendSegment = true)) dataExist = true
            (EMPTYSTR, null)
          case _ => node._1.substring(node._1.indexOf(".") + 1) == whichSeg match {
            case true => if (modelData(layout, model)(modelFilter, node._2.asInstanceOf[mapType])(dataHandler, appendSegment = true)) {
              dataExist = true
              (EMPTYSTR, null)
            } else (skippedStr, null)
            case _ => (EMPTYSTR, null)
          }

        }
      }
      catch {
        case t: Throwable => (null, t)
      }

    })
    (dataExist, temp)
  }


  private def makeFinal(layout: mutable.LinkedHashMap[String, String]): String = {
    val builder = new StringBuilder(layout.size * 40)
    layout.foreach({ case (k, v) => builder append (v + outDelim) })
    if (timeStampReq) builder append timeStamp
    builder.toString
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
            case Some(m: mapType) => m foreach { case (mk, mv) =>
              if (mk.substring(mk.indexOf(".") + 1) == ele._1.substring(ele._1.indexOf(".") + 1)) {
                mv match {
                  case str: String => layout update(ele._1, str)
                  case _ =>
                }
              }
            }

            case _ =>
          }
        }
      })
      case _ =>
    }

  }

  private def includeEle(underlying: mutable.LinkedHashMap[String, String], key: String, req: String, repeat: String = EMPTYSTR): Unit = {
    if (req == EMPTYSTR & repeat == EMPTYSTR) return
    underlying get key match {
      case Some(x) => x match {
        case EMPTYSTR => underlying update(key, req)
        case exists => if (repeat == EMPTYSTR) underlying update(key, exists + req)
        else underlying update(key, exists + repeat + req)
      }
      case k => throw new DataModelException(key + " Key Not Found while Applying Model to Data :: " + k)
    }

  }

  private def modelData(underlying: mutable.LinkedHashMap[String, String], model: Model)(filterKeys: Map[String, mutable.Set[String]], data: mapType)
                       (dataHandler: (String, String, String) => Unit, appendSegment: Boolean = false): Boolean = {
    var dataExist = false
    data.foreach(node => {
      node._1 != null & node._1 != EMPTYSTR & (filterKeys isDefinedAt node._1) match {
        case true => node._2 match {
          case str: String => if (underlying isDefinedAt node._1) {
            if (!appendSegment) dataHandler(node._1, str, EMPTYSTR)
            else dataHandler(node._1, str, repeat)
            dataExist = true
          }
          case map: mapType => handelMap(map, node._1, model)(filterKeys)(underlying, dataHandler, appendSegment)
          case list: listType => handleList(list, node._1, model)(filterKeys)(underlying, dataHandler, appendSegment)
          case any: Any => throw new DataModelException("Got an Unhandled Type into Filter Util ::  " + any.getClass + " ?? Not Yet Implemented")
        }
        case _ =>
      }
    })
    dataExist
  }


  private def handelMap(data: mapType, node: String, model: Model)(filterKeys: Map[String, mutable.Set[String]])
                       (underlying: mutable.LinkedHashMap[String, String], dataHandler: (String, String, String) => Unit, appendSegment: Boolean = false): Unit = {
    data.foreach({
      case (k, v) =>
        v match {
          case str: String =>
            if (underlying isDefinedAt (node + model.modelFieldDelim + k)) {
              if (!appendSegment) dataHandler(node + model.modelFieldDelim + k, str, EMPTYSTR)
              else dataHandler(node + model.modelFieldDelim + k, str, repeat)
            }
            else if ((underlying isDefinedAt node) & !isEmpty(node, filterKeys)) {
              if (!appendSegment) dataHandler(node, str, EMPTYSTR)
              else dataHandler(node, str, repeat)
            }
          case listType: listType => handleList(listType, node + model.modelFieldDelim + k, model)(filterKeys)(underlying, dataHandler, appendSegment)
          case map: mapType => handelMap(map, node + model.modelFieldDelim + k, model)(filterKeys)(underlying, dataHandler, appendSegment)
        }
    })
  }

  private def handleList(data: listType, node: String, model: Model)(filterKeys: Map[String, mutable.Set[String]])
                        (underlying: mutable.LinkedHashMap[String, String], dataHandler: (String, String, String) => Unit, appendSegment: Boolean = false): Unit = {
    data.foreach(map => map.foreach({ case (k, v) =>
      v match {
        case str: String =>
          if (underlying isDefinedAt (node + model.modelFieldDelim + k)) dataHandler(node + model.modelFieldDelim + k, str, repeat)
          else if ((underlying isDefinedAt node) & !isEmpty(node, filterKeys)) dataHandler(node, str, repeat)
        case list: listType => handleList(list, node + model.modelFieldDelim + k, model)(filterKeys)(underlying, dataHandler, appendSegment)
        case map: mapType => handelMap(map, node + model.modelFieldDelim + k, model)(filterKeys)(underlying, dataHandler, appendSegment)
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
