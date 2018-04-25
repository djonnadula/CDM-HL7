package com.hca.cdm.hl7.model

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.hca.cdm._
import com.hca.cdm.exception.CdmException
import com.hca.cdm.hl7.EnrichCacheManager
import com.hca.cdm.hl7.constants.HL7Constants._
import com.hca.cdm.hl7.constants.HL7Types.{withName => hl7, _}
import com.hca.cdm.hl7.enrichment.EnrichedData
import com.hca.cdm.hl7.filter.FilterUtility.{filterTransaction => filterRec}
import com.hca.cdm.hl7.model.OutFormats._
import com.hca.cdm.log.Logg
import com.hca.cdm.utils.DateUtil.{currentTimeStamp => timeStamp}
import scala.collection.mutable.{LinkedHashMap, Set}
import scala.language.postfixOps
import com.hca.cdm.kfka.config.HL7ProducerConfig.{createConfig => producerConf}
import com.hca.cdm.kfka.producer.{KafkaProducerHandler => KProducer}
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
  private lazy val filtered = new LinkedHashMap[String, Throwable] += (filteredStr -> null)
  private lazy val toJson = new ObjectMapper().registerModule(DefaultScalaModule).writer.writeValueAsString(_)
  lazy val kafkaOut = KProducer()(producerConf())
  lazy val hl7JsonIO = kafkaOut.writeData(_: String,"", lookUpProp("de-id.raw"))(4194304,null)
  def applyModel(whichSeg: String, model: Model)(data: mapType, rawHl7: String): Hl7SegmentTrans = {
    val modelFilter: Map[String, Set[String]] = model.modelFilter
    if (reqMsgType != IPLORU && reqMsgType != ORMORDERS && reqMsgType != IPLORDERS && reqMsgType != VENTORU && !isRequiredType(data, reqMsgType)) return notValid
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
                adhoc.reqNoAppends.foreach { field =>
                  if (layout.isDefinedAt(field) && layout(field).contains(caret)) layout update(field, layout(field).split(s"\\$caret", -1).find { data => valid(data) && data != EMPTYSTR }.getOrElse(EMPTYSTR))
                }
                handleCommonSegments(data, layout)
                val enrichedData = EnrichCacheManager().getEnRicher(adhoc transformer).applyTransformations(layout, rawHl7)
                if (enrichedData.rejects.nonEmpty) enrichedData.rejects.get foreach { rej => out._2 += partialRejectStr -> rej }
                adhoc.outFormat match {
                  case JSON | DELIMITED =>
                    handleEnrichData(adhoc.outFormat, model, adhoc, enrichedData)(out._2)
                    hl7JsonIO(enrichedData.enrichedHL7)
                  case RAWHL7 =>
                    out._2 += enrichedData.enrichedHL7+ "\r\n" -> null
                  case RAWHL7_JSON =>
                    handleEnrichData(adhoc.outFormat, model, adhoc, enrichedData)(out._2)
                    out._2 += enrichedData.enrichedHL7+ "\r\n" -> null
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
            data.map { case (node, struct) =>
              try {
                if (node.substring(node.indexOf(DOT) + 1) == model.reqSeg) {
                  layout = model.layoutCopy
                  if (modelData(layout, model)(modelFilter, struct.asInstanceOf[mapType])(dataHandler, appendSegment = true)) {
                    handleCommonSegments(data, layout)
                    if (layout isDefinedAt fieldSeqNum) layout update(fieldSeqNum, node.substring(0, node.indexOf(DOT)))
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
            }
        }
    }
    temp filter (_._1 != EMPTYSTR) match {
      case out => if (valid(out)) Hl7SegmentTrans(Left(out))
      else segmentDoesntExist
    }
  }

  private def handleEnrichData(outFormat: OutFormat, model: Model, adhoc: ADHOC, enriched: EnrichedData)(collector: LinkedHashMap[String, Throwable]): Unit = {
    def formatData(data: LinkedHashMap[String, String]): String = {
      outFormat match {
        case JSON =>
          val temp = model.adhocLayout(data, adhoc.outKeyNames, adhoc.multiColumnLookUp)
          if (timeStampReq) temp += timeStampKey -> timeStamp
          toJson(temp)
        case DELIMITED =>
          val temp = if (adhoc.outKeyNames.nonEmpty) model.adhocLayout(data, adhoc.outKeyNames, adhoc.multiColumnLookUp) else data
          if (timeStampReq) temp += timeStampKey -> timeStamp
          makeFinal(temp, etlTimeReq = false)
      }
    }

    enriched.enrichedLayout match {
      case oneUnit: LinkedHashMap[String, String] =>
        collector += formatData(oneUnit) -> null
      case multi: Traversable[LinkedHashMap[String, String]] =>
        multi foreach { d => collector += formatData(d) -> null }
      case notImpl => throw new CdmException(s"Not Yet Implemented for $notImpl")
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
  private def nodesTraversal(data: mapType, model: Model, layout: LinkedHashMap[String, String], modelFilter: Map[String, Set[String]],
                             dataHandler: (String, String, String) => Unit,
                             allNodes: Boolean = true, whichSeg: String = EMPTYSTR): (Boolean, LinkedHashMap[String, Throwable]) = {
    if (layout isEmpty) return (true, new LinkedHashMap[String, Throwable])
    var dataExist = false
    val temp = data.map { case (node, struct) =>
      try {
        if (allNodes) {
          if (node != commonNodeStr) if (modelData(layout, model)(modelFilter, struct.asInstanceOf[mapType])(dataHandler, appendSegment = true)) dataExist = true
          (EMPTYSTR, null)
        } else {
          if (node.substring(node.indexOf(DOT) + 1) == whichSeg) {
            if (modelData(layout, model)(modelFilter, struct.asInstanceOf[mapType])(dataHandler, appendSegment = true)) {
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

    }
    (dataExist, temp)
  }

  /**
    * Final Output Format of Transaction
    *
    * @param layout
    * @return
    */
  private def makeFinal(layout: LinkedHashMap[String, String], etlTimeReq: Boolean = timeStampReq): String = {
    var builder = layout.values.mkString(outDelim)
    if (etlTimeReq) builder += outDelim + timeStamp
    builder
  }

  private def handleCommonSegments(data: mapType, layout: LinkedHashMap[String, String]) = {
    if (data isDefinedAt commonNodeStr) data(commonNodeStr).asInstanceOf[LinkedHashMap[String, String]].foreach({
      case (k, v) =>
        if (layout isDefinedAt k) layout update(k, v)
    })
  }

  private def includeEle(underlying: LinkedHashMap[String, String], key: String, req: String, repeat: String = EMPTYSTR): Unit = {
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
  private def modelData(underlying: LinkedHashMap[String, String], model: Model)(filterKeys: Map[String, Set[String]], data: mapType)
                       (dataHandler: (String, String, String) => Unit, appendSegment: Boolean = false): Boolean = {
    var dataExist = false

    def applyData(key: String, data: String = EMPTYSTR): Unit = {
      if (underlying isDefinedAt key) {
        if (!appendSegment) dataHandler(key, data, EMPTYSTR)
        else dataHandler(key, data, caret)
        dataExist = true
      }
    }

    data.foreach { case (node, struct) =>
      if (node != null & node != EMPTYSTR & (filterKeys isDefinedAt node)) {
        struct match {
          case str: String => applyData(node, str)
          case None => applyData(node)
          case map: mapType =>
            if (handelMap(map, node, model)(filterKeys)(underlying, dataHandler, appendSegment)) dataExist = true
          case list: listType =>
            if (handleList(list, node, model)(filterKeys)(underlying, dataHandler, appendSegment)) dataExist = true
          case any: Any => throw new DataModelException("Got an Unhandled Type into Data Modeler " + logIdent + " :: " + any.getClass + " ?? Not Yet Implemented")
        }
      }
    }
    dataExist
  }

  private def handelMap(data: mapType, node: String, model: Model)(filterKeys: Map[String, Set[String]])
                       (underlying: LinkedHashMap[String, String], dataHandler: (String, String, String) => Unit, appendSegment: Boolean = false): Boolean = {
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

  private def handleList(data: listType, node: String, model: Model)(filterKeys: Map[String, Set[String]])
                        (underlying: LinkedHashMap[String, String], dataHandler: (String, String, String) => Unit, appendSegment: Boolean = false): Boolean = {
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

  private def isEmpty(key: String, filterKeys: Map[String, Set[String]]): Boolean = {
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
