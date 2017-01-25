package com.hca.cdm.hl7.parser

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.hca.cdm
import com.hca.cdm._
import com.hca.cdm.hl7.audit.AuditConstants._
import com.hca.cdm.hl7.audit._
import com.hca.cdm.hl7.constants.HL7Constants._
import com.hca.cdm.hl7.constants.HL7Types.HL7
import com.hca.cdm.hl7.constants.FileMappings._
import com.hca.cdm.hl7.exception.{InvalidHl7FormatException, InvalidTemplateFormatException, TemplateInfoException}
import com.hca.cdm.hl7.model.HL7State._
import com.hca.cdm.hl7.model._
import com.hca.cdm.hl7.validation.NotValidHl7Exception
import com.hca.cdm.hl7.validation.ValidationUtil.{hasMultiMSH => msgHasmultiMSH, isValidMsg => metRequirement}
import com.hca.cdm.log.Logg
import org.apache.commons.lang3.StringUtils.{leftPad => lPad}
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.language.postfixOps
import scala.util.control.Breaks._
import scala.util.{Failure, Success, Try}


/**
  * Created by Devaraj Jonnadula on 8/6/2016.
  *
  * Transforms Raw HL7 Message into Required Output formats as Per Templates Provided
  */
class HL7Parser(val msgType: HL7, private val templateData: Map[String, Map[String, Array[String]]]) extends Logg with Serializable {

  private lazy val toJson = new ObjectMapper().registerModule(DefaultScalaModule).writer.writeValueAsString(_)
  private lazy val EMPTY = Array.empty[String]
  private lazy val MAP = Map.empty[String, Array[String]]
  private val metrics = new TrieMap[String, Long]
  private val hl7 = msgType.toString
  HL7State.values.foreach(state => metrics += s"$hl7$COLON${state.toString}" -> 0L)
  info(
    hl7 + " " +
      """Template Registered For Parsing ::
      ____
    HL / /    DATA
      / /
     / /
    /_/
      """)

  @throws(classOf[IllegalArgumentException])
  @throws(classOf[AssertionError])
  @throws(classOf[InvalidTemplateFormatException])
  def transformHL7(hl7Message: String, invalidTemplateAlert: (String, String) => Unit): HL7TransRec = {
    require(hl7Message != null && !hl7Message.isEmpty, s"Error Nothing to parse $hl7Message")
    assume(isHL7(hl7Message), s"Not a Valid HL7. Check with Facility :: $hl7Message")
    val delim = if (hl7Message contains "\r\n") "\r\n" else "\n"
    Try(transform(hl7Message split delim)) match {
      case Success(parsed) =>
        handleCommonSegments(parsed.data)
        val meta = msgMeta(parsed.data)
        if (parsed.missingMappings != EMPTYSTR) {
          invalidTemplateAlert(rejectMsg(hl7, missingTemplate, meta, parsed.missingMappings, null, null, null), header(hl7, rejectStage, Left(meta)))
          updateMetrics(UNKNOWNMAPPING)
        }
        metRequirement(meta) status match {
          case Left(true) =>
            if (msgHasmultiMSH(parsed.data)) throw new InvalidHl7FormatException(s"Message has Multiple MSH Segments. This is not Expected.")
            Try(toJson(parsed.data)) match {
              case Success(json) =>
                updateMetrics(PROCESSED)
                HL7TransRec(Left((json, parsed.data, meta)))
              case Failure(t) =>
                error(s"Transforming to Json Failed for HL7 ${t.getMessage}", t)
                updateMetrics(FAILED)
                HL7TransRec(Right(t))
            }
          case Right(invalid) =>
            updateMetrics(REJECTED)
            invalid match {
              case (EMPTYSTR, EMPTYSTR) => throw new NotValidHl7Exception(invalidHl7)
              case (_, EMPTYSTR) => throw new NotValidHl7Exception(s"$invalidHl7 for ${invalid._1}")
              case (EMPTYSTR, _) => throw new NotValidHl7Exception(s"$invalidHl7 for ${invalid._2}")
            }
          case Left(false) => throw new RuntimeException("Invalid Case. This should not happen")
        }
      case Failure(t) =>
        t match {
          case x: TemplateInfoException =>
            updateMetrics(REJECTED)
          case _ =>
            updateMetrics(FAILED)
        }
        HL7TransRec(Right(t))
    }
  }

  def metricsRegistry: TrieMap[String, Long] = metrics

  def resetMetrics: Boolean = {
    this.metrics.synchronized {
      metrics.transform((k, v) => if (v != 0L) 0L else v)
    }
    true
  }

  def overSizeMsgFound(): Unit = updateMetrics(OVERSIZED)

  private def updateMetrics(state: hl7State) = {
    val key = s"$hl7$COLON$state"
    metrics get key match {
      case Some(stat) => metrics update(key, cdm.inc(stat))
      case _ => throw new DataModelException(s"Cannot Update Metrics Key not Found. $logIdent This should not Happen :: $state")
    }
  }

  private def findReqSegment(data: mapType, reqSeg: String): String = {
    var segment = EMPTYSTR
    breakable {
      data.foreach(node => {
        if (node._1.substring(node._1.indexOf(DOT) + 1) == reqSeg) {
          segment = node._1
          break
        }
      })
    }
    segment
  }

  private def handleCommonSegments(data: mapType) = {
    val mshNode = findReqSegment(data, MSH)
    val pidNode = findReqSegment(data, PID)
    val commonNode = data(commonNodeStr).asInstanceOf[mutable.LinkedHashMap[String, String]]
    MSHMappings.foreach(key => handleIndexes(mshNode, data, MSHMappings(key._1), key._1, commonNode))
    PIDMappings.foreach(key => handleIndexes(pidNode, data, PIDMappings(key._1), key._1, commonNode))
  }

  private def handleIndexes(segIndex: String, data: mapType, path: Array[(String, String, String, String)], whichKey: String, nodeToUpdate: mutable.LinkedHashMap[String, String]) = {
    data.get(segIndex) match {
      case Some(node) => nodeToUpdate update(whichKey, findData(node, path))
      case _ =>
    }
  }

  private def findData(data: Any, path: Array[(String, String, String, String)]): String = {
    data match {
      case maptype: mapType =>
        path headOption match {
          case Some(x) =>
            if (maptype isDefinedAt x._1) {
              maptype(x._1) match {
                case str: String =>
                  if (str != EMPTYSTR) str
                  else findData(data, path tail)
                case subMap: mapType =>
                  if (subMap isDefinedAt x._2) {
                    subMap(x._2) match {
                      case s: String =>
                        if (s != EMPTYSTR) {
                          if (x._3 != EMPTYSTR && subMap.isDefinedAt(x._3)) {
                            if (subMap(x._3) == x._4) s
                            else findData(data, path tail)
                          } else s
                        } else findData(data, path tail)
                      case _ => findData(data, path tail)
                    }
                  }
                  else findData(data, path tail)
                case list: listType =>
                  var temp = EMPTYSTR
                  breakable {
                    list.foreach(node => {
                      if (node isDefinedAt x._2) {
                        node(x._2) match {
                          case s: String =>
                            if (s != EMPTYSTR) {
                              if (x._3 != EMPTYSTR && node.isDefinedAt(x._3)) {
                                if (node(x._3) == x._4) {
                                  temp = s
                                  break
                                }
                              } else {
                                temp = s
                                break
                              }
                            }
                          case _ =>
                        }
                      }

                    })
                  }
                  if (temp == EMPTYSTR) findData(data, path tail)
                  else temp
              }
            }
            else findData(data, path tail)
          case _ => EMPTYSTR
        }
      case _ => EMPTYSTR
    }
  }

  private case class Segments(var componentData: String = EMPTYSTR, var realignColStatus: Boolean = false, var realignColValue: String = EMPTYSTR,
                              var realignFieldName: String = EMPTYSTR, var realignCompName: String = EMPTYSTR, var realignSubCompName: String = EMPTYSTR, var realignColOption: String = EMPTYSTR)

  private case class VersionData(var controlId: String = EMPTYSTR, var hl7Version: String = EMPTYSTR, var srcSystem: Map[String, Array[String]] = MAP,
                                 var standardMapping: Map[String, Array[String]] = MAP, var realignment: Map[String, Array[String]] = MAP)

  private case class HL7Parsed(data: mapType, sourceSystem: String, srcVersion: String, var missingMappings: String = EMPTYSTR)

  private case class TemplateUnknownMapping(var unknownMappings: ListBuffer[String] = null)

  private def transform(rawSplit: Array[String], preNumLen: Int = 4) = {
    val dataLayout = new mutable.LinkedHashMap[String, Any]
    dataLayout += commonNodeStr -> commonNode.clone().transform((k, v) => if (v ne EMPTYSTR) EMPTYSTR else v)
    val delimiters = new mutable.HashMap[String, String]()
    if (!(rawSplit(0) startsWith MSH)) throw new InvalidHl7FormatException(s"Expecting First Segment in HL7 as MSH but found :: ${rawSplit(0)}")
    delimiters.put(FIELD_DELIM, if (rawSplit(0).charAt(3) + EMPTYSTR == PIPE_DELIMITED) "|" else (rawSplit(0).charAt(3) + EMPTYSTR).trim)
    delimiters.put(CMPNT_DELIM, if (rawSplit(0).charAt(4) + EMPTYSTR == caret) "^" else (rawSplit(0).charAt(4) + EMPTYSTR).trim)
    delimiters.put(REPTN_DELIM, (rawSplit(0).charAt(5) + EMPTYSTR).trim)
    delimiters.put(ESC_DELIM, if (rawSplit(0).charAt(6) + EMPTYSTR == ESCAPE) ESCAPE else (rawSplit(0).charAt(6) + EMPTYSTR).trim)
    delimiters.put(SUBCMPNT_DELIM, (rawSplit(0).charAt(7) + EMPTYSTR).trim)
    delimiters.put(TRUNC_DELIM, if (rawSplit(0).charAt(8) + EMPTYSTR != PIPE_DELIMITED) (rawSplit(0).charAt(8) + EMPTYSTR).trim else "//")
    var realignColStatus = false
    var segments: Segments = new Segments
    var realignVal = EMPTYSTR
    var versionData: VersionData = null
    var missingMappings = new TemplateUnknownMapping
    val segmentsMapping = mapSegments(segments, _: String, versionData.controlId.substring(0, versionData.controlId.indexOf("_")) + "_" + versionData.hl7Version + "_"
      , versionData.srcSystem, versionData.standardMapping, versionData.realignment)(versionData.controlId, missingMappings)
    var i = 0
    rawSplit foreach { msgSegment =>
      i = inc(i)
      val whichSegment = msgSegment substring(0, 3)
      val segmentIndex = s"${lPad(s"$i$EMPTYSTR", preNumLen, ZEROStr)}$DOT$whichSegment"
      val componentLayout = mutable.LinkedHashMap[String, Any]()
      var fields: Array[String] = null
      whichSegment == MSH match {
        case true =>
          fields = msgSegment.split(ESCAPE + delimiters(FIELD_DELIM))
          versionData = getVersionData(fields, templateData)
          fields(0) = delimiters(FIELD_DELIM)
        case _ =>
          if (msgSegment.length > 4) fields = msgSegment.substring(4, msgSegment.length).split(ESCAPE + delimiters(FIELD_DELIM))
          else fields = msgSegment.split(ESCAPE + delimiters(FIELD_DELIM))
      }
      val moveToUnknown = moveUnknown(componentLayout, unknownKey(whichSegment), _: String)
      valid(fields) match {
        case true =>
          var j = 0
          fields foreach { field =>
            j = inc(j)
            if (nonEmpty(field)) {
              val componentIndex = s"$whichSegment$DOT$j"
              segments = segmentsMapping(componentIndex)._1
              val componentData = segments.componentData
              !(whichSegment == MSH && j == 2) match {
                case true =>
                  val multiFields = if (whichSegment == OBX_SEG && j == 5) Array(field) else field.split(ESCAPE + delimiters(REPTN_DELIM), -1)
                  val componentList = new mutable.ListBuffer[Any]
                  breakable {
                    multiFields foreach { fieldRepeatItem =>
                      val subComponent = fieldRepeatItem.split(ESCAPE + delimiters(CMPNT_DELIM), -1)
                      if (subComponent.length > 1) {
                        val subComponentLayout = new mutable.LinkedHashMap[String, Any]()
                        var subComponentData = EMPTYSTR
                        var k = 0
                        subComponent foreach { subComp =>
                          k = inc(k)
                          if (nonEmpty(subComp)) {
                            val subComponentIndex = s"$componentIndex$DOT$k"
                            segments = segmentsMapping(subComponentIndex)._1
                            subComponentData = segments.componentData
                            var subSubComponent: Array[String] = EMPTY
                            realignColStatus = segments.realignColStatus
                            realignColStatus match {
                              case true => segments.realignColOption match {
                                case MERGE =>
                                  if (realignVal == EMPTYSTR) realignVal = subComp
                                  else realignVal = realignVal + subComp
                                case MOVE =>
                                  val subCompRep = subComp.replace(delimiters(CMPNT_DELIM), delimiters(SUBCMPNT_DELIM))
                                  subComponentData = segments.realignCompName
                                  subSubComponent = subCompRep.split(ESCAPE + delimiters(SUBCMPNT_DELIM), -1)
                                case _ =>
                              }
                              case _ =>
                                subSubComponent = subComp.split(ESCAPE + delimiters(SUBCMPNT_DELIM), -1)
                            }
                            subSubComponent.nonEmpty & subSubComponent.length > 1 match {
                              case true =>
                                val subSubComponentLayout = new mutable.LinkedHashMap[String, Any]()
                                var l = 0
                                subSubComponent foreach { msgSubSubComponent =>
                                  l = inc(l)
                                  if (nonEmpty(msgSubSubComponent)) {
                                    segments = segmentsMapping(s"$subComponentIndex$DOT$l")._1
                                    if (segments.componentData == UNKNOWN) moveToUnknown(msgSubSubComponent)
                                    else subSubComponentLayout += segments.componentData -> msgSubSubComponent
                                  }
                                }
                                if (subSubComponentLayout nonEmpty) subComponentLayout += subComponentData -> subSubComponentLayout
                                if (realignColStatus & segments.realignColOption == MOVE) {
                                  componentLayout += componentData -> subComponentLayout
                                  break
                                }
                              case _ =>
                                if (subComponentData == UNKNOWN) moveToUnknown(subComp)
                                else subComponentLayout += subComponentData -> subComp
                            }
                          }
                        }
                        realignColStatus match {
                          case true =>
                            if (componentData == UNKNOWN) moveToUnknown(realignVal)
                            else componentLayout += componentData -> realignVal
                            segments.realignColValue = EMPTYSTR
                            realignColStatus = false
                            realignVal = EMPTYSTR
                          case _ =>
                            if (multiFields.length > 1) componentList += subComponentLayout
                            else if (subComponentLayout nonEmpty) componentLayout += componentData -> subComponentLayout
                        }
                      }
                      else {
                        if (componentData == UNKNOWN) moveToUnknown(field)
                        else componentLayout += componentData -> field
                      }
                    }
                  }
                  if (multiFields.length > 1) {
                    componentLayout += componentData -> componentList
                  }
                case _ =>
                  segments = segmentsMapping(componentIndex)._1
                  if (segments.componentData == UNKNOWN) moveToUnknown(field)
                  else componentLayout += segments.componentData -> field
              }
            }
          }
        case _ =>
      }
      dataLayout += segmentIndex -> componentLayout
    }
    missingMappings.unknownMappings match {
      case null =>
        HL7Parsed(dataLayout, versionData.controlId.substring(0, versionData.controlId.indexOf("_")), versionData.hl7Version)
      case _ =>
        HL7Parsed(dataLayout, versionData.controlId.substring(0, versionData.controlId.indexOf("_")), versionData.hl7Version,
          s"Template Don't have mappings for ${missingMappings.unknownMappings.mkString(s"$COLON$COLON")} & Source System Version ${versionData.controlId.substring(0, versionData.controlId.indexOf("_"))}-${versionData.hl7Version} & Msg Control Id ${versionData.controlId}")
    }
  }

  private def moveUnknown(layout: mapType, mapping: String, data: String): Unit = {
    addUnknownEntry(mapping, layout)
    handleUnKnown(mapping, data, layout)
  }

  private def handleUnKnown(mapping: String, data: String, underlying: mapType): Unit = {
    underlying isDefinedAt mapping match {
      case true =>
        if (underlying(mapping) == EMPTYSTR) underlying update(mapping, data)
        else underlying update(mapping, s"${underlying(mapping)}$caret$data")
      case _ =>
    }
  }

  private def addUnknownEntry(unknown: String, data: mapType): Unit = {
    if (!(data isDefinedAt unknown)) data += unknown -> EMPTYSTR
  }

  private def unknownKey(segment: String): String = s"${segment.toLowerCase}_$UNKNOWN"

  private def inc(v: Int, step: Int = 1) = v + step

  private def getVersionData(fieldList: Array[String], templateData: Map[String, Map[String, Array[String]]]): VersionData = {
    val hl7Version = fieldList(11)
    val controlId = fieldList(9)
    val Match = matcher(controlId, _: String)
    val mapped_index = Match match {
      case mt_mt6 if mt_mt6(MT_) | mt_mt6(MT6_) =>
        if (hl7Version == HL7_2_1) templateData(hl7MEDITECH21Map)
        else if (hl7Version == HL7_2_4 | hl7Version == HL7_2_5_1) templateData(hl7MEDITECH24Map)
        else templateData(hl7StandardMap)
      case epic if epic(EPIC_) => templateData(hl7EpicMap)
      case ecw if ecw(ECW_) => templateData(hl7eCWMap)
      case ng if ng(NG_) => templateData(hl7NextGenMap)
      case ip if ip(IP_) => templateData(hl7IpeopleMap)
      case _ => MAP
    }
    VersionData(controlId, hl7Version, mapped_index, templateData(hl7StandardMap), templateData(hl7MapAlignXWalk))
  }

  private def matcher(in: String, seq: String) = in != null & in.contains(seq)


  private def mapSegments(segment: Segments, segmentIndex: String, controlVersion: String, srcSystemMapping: Map[String, Array[String]]
                          , standardMapping: Map[String, Array[String]], realignment: Map[String, Array[String]])(controlId: String, missingMappings: TemplateUnknownMapping): (Segments, TemplateUnknownMapping) = {
    val segments = resetSegment(segment)
    var mappedColumnData = EMPTYSTR
    try {
      val realignColumnValues = lookUp(realignment, controlVersion + segmentIndex)
      realignColumnValues.nonEmpty match {
        case true =>
          segments.realignColStatus = true
          segments.realignFieldName = realignColumnValues(7)
          segments.realignCompName = realignColumnValues(8)
          segments.realignSubCompName = realignColumnValues(9)
          if (mappingExist(9, realignColumnValues) && nonEmpty(realignColumnValues(9))) mappedColumnData = realignColumnValues(9)
          else if (mappingExist(8, realignColumnValues) && nonEmpty(realignColumnValues(8))) mappedColumnData = realignColumnValues(8)
          else if (mappingExist(7, realignColumnValues) && nonEmpty(realignColumnValues(7))) mappedColumnData = realignColumnValues(7)
          segments.realignColOption = realignColumnValues(10)
          segments.componentData = mappedColumnData
          (segment, missingMappings)
        case _ =>
          segments.realignColStatus = false
          val standardMappedValues = lookUp(standardMapping, segmentIndex)
          val srcSystemValues = lookUp(srcSystemMapping, segmentIndex)
          mappedColumnData = segmentIndex
          standardMappedValues.nonEmpty match {
            case true =>
              if (mappingExist(5, standardMappedValues) && nonEmpty(standardMappedValues(5))) mappedColumnData = standardMappedValues(5)
              else if (mappingExist(4, standardMappedValues) && nonEmpty(standardMappedValues(4))) mappedColumnData = standardMappedValues(4)
              else if (mappingExist(3, standardMappedValues) && nonEmpty(standardMappedValues(3))) mappedColumnData = standardMappedValues(3)
              else if (mappingExist(2, standardMappedValues) && nonEmpty(standardMappedValues(2))) mappedColumnData = standardMappedValues(2)
              else if (mappingExist(1, standardMappedValues) && nonEmpty(standardMappedValues(1))) mappedColumnData = standardMappedValues(1)
              else if (mappingExist(0, standardMappedValues) && nonEmpty(standardMappedValues(0))) mappedColumnData = standardMappedValues(0)
            case _ =>
          }
          srcSystemValues.nonEmpty match {
            case true =>
              if (mappingExist(2, srcSystemValues) && nonEmpty(srcSystemValues(2))) mappedColumnData = srcSystemValues(2)
              else if (mappingExist(1, srcSystemValues) && nonEmpty(srcSystemValues(1))) mappedColumnData = srcSystemValues(1)
              else if (mappingExist(0, srcSystemValues) && nonEmpty(srcSystemValues(0))) mappedColumnData = srcSystemValues(0)
            case _ =>
          }
          if (mappedColumnData == EMPTYSTR || mappedColumnData.contains(DOT) ||
            (mappedColumnData.length > 2 && mappedColumnData.substring(3) == segmentIndex.substring(3))) {
            mappedColumnData = UNKNOWN
            if (missingMappings.unknownMappings == null) missingMappings.unknownMappings = new ListBuffer[String]
            missingMappings.unknownMappings += segmentIndex
          }
          segments.componentData = mappedColumnData
          (segments, missingMappings)
      }
    } catch {
      case t: Throwable =>
        throw new InvalidTemplateFormatException(s"Template has invalid Format for $segmentIndex & Source System Version $controlVersion & Msg Control Id $controlId . Cannot Apply Template Schema. Correct templates", t)
    }
  }

  private def resetSegment(segment: Segments): Segments = {
    segment.componentData = EMPTYSTR
    segment.realignColStatus = false
    segment.realignColValue = EMPTYSTR
    segment.realignColOption = EMPTYSTR
    segment.realignFieldName = EMPTYSTR
    segment.realignCompName = EMPTYSTR
    segment.realignSubCompName = EMPTYSTR
    segment.realignColOption = EMPTYSTR
    segment
  }

  private def nonEmpty(key: String) = key != null & key != EMPTYSTR

  private def mappingExist(index: Int, template: Array[String]) = template != null && index > -1 && template.isDefinedAt(index)

  private def isHL7(message: String) = matcher(message, MSH)

  private def lookUp(store: Map[String, Array[String]], key: String) = {
    store get key match {
      case Some(data) => data
      case _ => EMPTY
    }
  }

  override def toString: String = s"HL7Parser($hl7, $metrics)"
}

object HL7Parser {

  def apply(msgType: HL7, templateMappings: Map[String, Map[String, Array[String]]]): HL7Parser =
    new HL7Parser(msgType, templateMappings)

}