package com.hca.cdm.hl7.model

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.hca.cdm._
import com.hca.cdm.hl7.constants.HL7Constants._
import com.hca.cdm.hl7.constants.HL7Types.{withName => hl7, _}
import com.hca.cdm.hl7.filter.FilterUtility.{filterTransaction => filterRec}
import com.hca.cdm.hl7.model.OutFormats._
import com.hca.cdm.log.Logg
import com.hca.cdm.utils.DateUtil.{currentTimeStamp => timeStamp}
import scala.collection.mutable

/**
  * Created by Devaraj Jonnadula on 8/18/2016.
  *
  * Breaks HL7 Data at Segment level and applies Schema for each Registered Segments and Special Cases for SCRI, CDI projects...
  */
private[model] class DataModeler(private val reqMsgType: HL7, private val timeStampReq: Boolean = true, private val outDelim: String = PIPE_DELIMITED_STR)
  extends Logg with Serializable {
  logIdent = "for HL7 Type :: " + reqMsgType.toString
  private lazy val notValid = Hl7SegmentTrans(Right(notValidStr))
  private lazy val segmentDoesntExist = Hl7SegmentTrans(Right(NA))
  private lazy val filtered = new mutable.LinkedHashMap[String, Throwable] += (filteredStr -> null)
  private lazy val toJson = new ObjectMapper().registerModule(DefaultScalaModule).writer.writeValueAsString(_)

  def applyModel(whichSeg: String, model: Model)(data: mapType, rawHl7: String): Hl7SegmentTrans = {
    val modelFilter: Map[String, mutable.Set[String]] = model.modelFilter
    if (modelFilter.isEmpty | (reqMsgType != IPLORU && reqMsgType != ORMORDERS && !isRequiredType(data, reqMsgType))) return notValid
    var layout = model.EMPTY
    val dataHandler = includeEle(layout, _: String, _: String, _: String)
    val temp = model.adhoc match {
      case Some(adhoc) =>
        if (filterRec(model.filters.get)(data)) {
          layout = model.layoutCopy
          nodesTraversal(data, model, layout, modelFilter, dataHandler) match {
            case out =>
              if (out._1) {
                // Case to Strip Out Fields which require Only One Occurrence
                adhoc.reqNoAppends.foreach(field =>
                  if (layout.isDefinedAt(field) && layout(field).contains(caret)) layout update(field, layout(field).substring(0, layout(field).indexOf(caret))))
                adhoc.outFormat match {
                  case JSON =>
                    handleCommonSegments(data, layout)
                    adhoc.transformer.foreach(_.applyTransformations(layout))
                    val temp = model.adhocLayout(layout, adhoc.outKeyNames, adhoc.multiColumnLookUp)
                    if (timeStampReq) temp += ((timeStampKey, timeStamp))
                    out._2 += (toJson(temp) -> null)
                  case DELIMITED =>
                    handleCommonSegments(data, layout)
                    out._2 += (makeFinal(layout) -> null)
                  case RAWHL7 =>
                    rawHl7 -> null
                }
              }
              out._2
          }
        } else {
          filtered
        }
      case None =>
        model.reqSeg match {
          // Case to Handle Segments into one Row
          /* case OBX_SEG =>
            layout = model.layoutCopy
            nodesTraversal(data, model, layout, modelFilter, dataHandler, allNodes = false, OBX_SEG) match {
              case out => if (out._1) {
                handleCommonSegments(data, layout)
                out._2 += (makeFinal(layout) -> null)
              }
                out._2
            } */
          case _ =>
            data.map(node => {
              try {
                if (node._1.substring(node._1.indexOf(DOT) + 1) == model.reqSeg) {
                  layout = model.layoutCopy
                  if (modelData(layout, model)(modelFilter, node._2.asInstanceOf[mapType])(dataHandler, appendSegment = true)) {
                    handleCommonSegments(data, layout)
                    if (layout isDefinedAt fieldSeqNum) layout update(fieldSeqNum, node._1.substring(0, node._1.indexOf(DOT)))
                    if (layout isDefinedAt timeStampKey) layout update(timeStampKey, timeStamp)
                    (makeFinal(layout, etlTimeReq = false), null)
                  } else {
                    (skippedStr, null)
                  }
                } else {
                  (EMPTYSTR, null)
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

  /**
    *
    * Traverses all nodes in data set and pulls Data Required as per Schema
    *
    * @param data
    * @param model
    * @param layout
    * @param modelFilter
    * @param dataHandler
    * @param allNodes
    * @param whichSeg
    * @return
    */
  private def nodesTraversal(data: mapType, model: Model, layout: mutable.LinkedHashMap[String, String], modelFilter: Map[String, mutable.Set[String]],
                             dataHandler: (String, String, String) => Unit, allNodes: Boolean = true, whichSeg: String = EMPTYSTR) = {
    var dataExist = false
    val temp = data.map(node => {
      try {
        if (allNodes) {
          if (node._1 != commonNodeStr) if (modelData(layout, model)(modelFilter, node._2.asInstanceOf[mapType])(dataHandler, appendSegment = true)) dataExist = true
          (EMPTYSTR, null)
        } else {
          if (node._1.substring(node._1.indexOf(DOT) + 1) == whichSeg) {
            if (modelData(layout, model)(modelFilter, node._2.asInstanceOf[mapType])(dataHandler, appendSegment = true)) {
              dataExist = true
              (EMPTYSTR, null)
            } else (skippedStr, null)
          } else {
            (EMPTYSTR, null)
          }
        }
      }
      catch {
        case t: Throwable => (null, t)
      }

    })
    (dataExist, temp)
  }

  /**
    * Final Output Format of Transaction
    *
    * @param layout
    * @return
    */
  private def makeFinal(layout: mutable.LinkedHashMap[String, String], etlTimeReq: Boolean = timeStampReq): String = {
    val builder = new StringBuilder(layout.size * 40)
    layout.foreach({ case (k, v) => builder append (v + outDelim) })
    if (etlTimeReq) builder append timeStamp
    builder.toString
  }

  private def handleCommonSegments(data: mapType, layout: mutable.LinkedHashMap[String, String]) = {
    if (data isDefinedAt commonNodeStr) data(commonNodeStr).asInstanceOf[mutable.LinkedHashMap[String, String]].foreach({
      case (k, v) =>
        if (layout isDefinedAt k) layout update(k, v)
    })
  }

  private def includeEle(underlying: mutable.LinkedHashMap[String, String], key: String, req: String, repeat: String = EMPTYSTR): Unit = {
    if (req == EMPTYSTR && repeat == EMPTYSTR) return
    underlying get key match {
      case Some(x) => x match {
        case EMPTYSTR => underlying update(key, req)
        case exists => if (repeat == EMPTYSTR) underlying update(key, exists + req)
        else underlying update(key, exists + repeat + req)
      }
      case k => throw new DataModelException(key + " Key Not Found while Applying Model to Data" + logIdent + " :: " + k)
    }

  }

  /** Traverses Respective Node in data set and pulls Data Required as per Schema
    *
    * @param underlying
    * @param model
    * @param filterKeys
    * @param data
    * @param dataHandler
    * @param appendSegment
    * @return
    */
  private def modelData(underlying: mutable.LinkedHashMap[String, String], model: Model)(filterKeys: Map[String, mutable.Set[String]], data: mapType)
                       (dataHandler: (String, String, String) => Unit, appendSegment: Boolean = false): Boolean = {
    var dataExist = false

    def applyData(key: String, data: String = EMPTYSTR): Unit = {
      if (underlying isDefinedAt key) {
        if (!appendSegment) dataHandler(key, data, EMPTYSTR)
        else dataHandler(key, data, caret)
        dataExist = true
      }
    }

    data.foreach(node => {
      if (node._1 != null & node._1 != EMPTYSTR & (filterKeys isDefinedAt node._1)) {
        node._2 match {
          case str: String => applyData(node._1, str)
          case None => applyData(node._1)
          case map: mapType =>
            if (handelMap(map, node._1, model)(filterKeys)(underlying, dataHandler, appendSegment)) dataExist = true
          case list: listType =>
            if (handleList(list, node._1, model)(filterKeys)(underlying, dataHandler, appendSegment)) dataExist = true
          case any: Any => throw new DataModelException("Got an Unhandled Type into Data Modeler " + logIdent + " :: " + any.getClass + " ?? Not Yet Implemented")
        }
      }
    })
    dataExist
  }

  private def handelMap(data: mapType, node: String, model: Model)(filterKeys: Map[String, mutable.Set[String]])
                       (underlying: mutable.LinkedHashMap[String, String], dataHandler: (String, String, String) => Unit, appendSegment: Boolean = false): Boolean = {
    var dataExist = false

    def applyData(key: String, data: String = EMPTYSTR): Unit = {
      if (underlying isDefinedAt (node + model.modelFieldDelim + key)) {
        if (!appendSegment) dataHandler(node + model.modelFieldDelim + key, data, EMPTYSTR)
        else dataHandler(node + model.modelFieldDelim + key, data, caret)
        dataExist = true
      }
      else if ((underlying isDefinedAt node) & !isEmpty(node, filterKeys)) {
        if (!appendSegment) dataHandler(node, data, EMPTYSTR)
        else dataHandler(node, data, caret)
        dataExist = true
      }
    }

    data.foreach({
      case (k, v) =>
        v match {
          case str: String => applyData(k, str)
          case None => applyData(k)
          case listType: listType => handleList(listType, node + model.modelFieldDelim + k, model)(filterKeys)(underlying, dataHandler, appendSegment)
          case map: mapType => handelMap(map, node + model.modelFieldDelim + k, model)(filterKeys)(underlying, dataHandler, appendSegment)
        }
    })
    dataExist
  }

  private def handleList(data: listType, node: String, model: Model)(filterKeys: Map[String, mutable.Set[String]])
                        (underlying: mutable.LinkedHashMap[String, String], dataHandler: (String, String, String) => Unit, appendSegment: Boolean = false): Boolean = {
    var dataExist = false

    def applyData(key: String, data: String = EMPTYSTR): Unit = {
      if (underlying isDefinedAt (node + model.modelFieldDelim + key)) {
        dataHandler(node + model.modelFieldDelim + key, data, caret)
        dataExist = true
      }
      else if ((underlying isDefinedAt node) & !isEmpty(node, filterKeys)) {
        dataHandler(node, data, caret)
        dataExist = true
      }
    }

    data.foreach(map => map.foreach({ case (k, v) =>
      v match {
        case str: String => applyData(k, str)
        case None => applyData(k)
        case list: listType => handleList(list, node + model.modelFieldDelim + k, model)(filterKeys)(underlying, dataHandler, appendSegment)
        case map: mapType => handelMap(map, node + model.modelFieldDelim + k, model)(filterKeys)(underlying, dataHandler, appendSegment)
      }
    }))
    dataExist
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
        case Some(seg) => seg.asInstanceOf[mapType].get(Message_Code) match {
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
