package com.hca.cdm.hl7.parser

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
import com.hca.cdm.hl7.overrides.OverrideHandle
import com.hca.cdm.hl7.overrides.overrides._
import com.hca.cdm.hl7.validation.NotValidHl7Exception
import com.hca.cdm.hl7.validation.ValidationUtil.{hasMultiMSH => msgHasmultiMSH, isValidMsg => metRequirement}
import com.hca.cdm.log.Logg
import org.apache.commons.lang3.StringUtils.{leftPad => lPad}
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.language.postfixOps
import scala.util.control.Breaks._
import scala.util.{Failure, Success, Try}


/**
  * Created by Devaraj Jonnadula on 8/6/2016.
  *
  * Transforms Raw HL7 Message into Required Output formats as Per Templates Provided
  */
class HL7Parser(val msgType: HL7, private val templateData: Map[String, Map[String, Array[String]]], filterNullMappings: Boolean = false) extends Logg with Serializable {

  require(msgType != null, s"Cannot Register Parser for $msgType")
  require(templateData != null, s"Cannot Register Parser with Templates  $templateData")
  private lazy val toJson = jsonHandler(filterNullMappings)
  private lazy val EMPTY = Array.empty[String]
  private lazy val NONE = Map.empty[String, Array[String]]
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
  def transformHL7(hl7Message: String, invalidTemplateAlert: (AnyRef, String) => Unit): HL7TransRec = {
    require(hl7Message != null && !hl7Message.isEmpty, s"Error Nothing to parse $hl7Message")
    assume(isHL7(hl7Message), s"Not a Valid HL7. Check with Facility :: $hl7Message")
    val delim = if (hl7Message contains "\r\n") "\r\n" else "\n"
    Try(transform(hl7Message split delim)) match {
      case Success(parsed) =>
        handleGtMriOverride(parsed.data)
        handleCommonSegments(parsed.data)
        val meta = msgMeta(parsed.data)
        if (parsed.missingMappings != EMPTYSTR) {
          invalidTemplateAlert(rejectMsg(hl7, missingTemplate, meta, parsed.missingMappings, null, null, null), header(hl7, rejectStage, Left(meta)))
          updateMetrics(UNKNOWNMAPPING)
        }
        metRequirement(meta) status match {
          case Left(true) =>
            if (msgHasmultiMSH(parsed.data)) throw new InvalidHl7FormatException(s"Message has Multiple MSH Segment. This is not Expected.")
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
          case _: TemplateInfoException =>
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
                case None => findData(data, path tail)
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

  private def handleGtMriOverride(data: mapType, handle: OverrideHandle = Interface_GtMriOverride()): Unit = {
    val mshNode = findReqSegment(data, MSH)
    if (!handle.isInterfaceSpecific(data(mshNode).asInstanceOf[mapType].getOrElse(Control_Id_key, EMPTYSTR).asInstanceOf[String])) return
    val pidNode = findReqSegment(data, PID)
    if (!(handle.mappings.map(key => findData(data(pidNode), key._2)).toSet.size > 1)) {
      handle.applyOverride(data(pidNode).asInstanceOf[mapType])
    }
  }

  private case class Segment(var mapping: String = EMPTYSTR, var realignColStatus: Boolean = false, var realignColValue: String = EMPTYSTR,
                             var realignFieldName: String = EMPTYSTR, var realignCompName: String = EMPTYSTR, var realignSubCompName: String = EMPTYSTR, var realignColOption: String = EMPTYSTR)

  private case class VersionData(var controlId: String = EMPTYSTR, var hl7Version: String = EMPTYSTR, facility: String = EMPTYSTR, var srcSystem: Map[String, Array[String]] = NONE,
                                 var standardMapping: Map[String, Array[String]] = NONE, var realignment: Map[String, Array[String]] = NONE, var facilityOverRides: Map[String, Array[String]] = NONE)

  private case class HL7Parsed(data: mapType, sourceSystem: String, srcVersion: String, var missingMappings: String = EMPTYSTR)

  private case class TemplateUnknownMapping(var unknownMappings: mutable.HashSet[String] = null)

  private def transform(rawSplit: Array[String], preNumLen: Int = 4) = {
    val dataLayout = new mutable.LinkedHashMap[String, Any]
    dataLayout += commonNodeStr -> commonNode.clone().transform((k, v) => if (v ne EMPTYSTR) EMPTYSTR else v)
    val delimiters = new mutable.HashMap[String, String]()
    if (!(rawSplit(0) startsWith MSH)) throw new InvalidHl7FormatException(s"Expecting First Segment in HL7 as MSH but found :: ${rawSplit(0)}")
    delimiters.put(FIELD_DELIM, if (rawSplit(0).charAt(3) + EMPTYSTR == PIPE_DELIMITED_STR) "|" else (rawSplit(0).charAt(3) + EMPTYSTR).trim)
    delimiters.put(CMPNT_DELIM, if (rawSplit(0).charAt(4) + EMPTYSTR == caret) "^" else (rawSplit(0).charAt(4) + EMPTYSTR).trim)
    delimiters.put(REPTN_DELIM, (rawSplit(0).charAt(5) + EMPTYSTR).trim)
    delimiters.put(ESC_DELIM, if (rawSplit(0).charAt(6) + EMPTYSTR == ESCAPE) ESCAPE else (rawSplit(0).charAt(6) + EMPTYSTR).trim)
    delimiters.put(SUBCMPNT_DELIM, (rawSplit(0).charAt(7) + EMPTYSTR).trim)
    delimiters.put(TRUNC_DELIM, if (rawSplit(0).charAt(8) + EMPTYSTR != PIPE_DELIMITED_STR) (rawSplit(0).charAt(8) + EMPTYSTR).trim else "//")
    var segment: Segment = null
    var versionData: VersionData = null
    val missingMappings = new TemplateUnknownMapping
    val segmentMapping = getMapping(_: String, versionData.controlId.substring(0, versionData.controlId.indexOf(underScore)) + underScore + versionData.hl7Version + underScore,
      versionData.controlId.substring(0, versionData.controlId.indexOf(underScore)) + underScore + versionData.facility + underScore + versionData.hl7Version + underScore
      , versionData.srcSystem, versionData.standardMapping, versionData.realignment, versionData.facilityOverRides)(versionData.controlId, missingMappings)
    rawSplit.view.zipWithIndex foreach { case (msgSegment, segIndex) =>
      val whichSegment = msgSegment substring(0, 3)
      val segmentIndex = s"${lPad(s"${inc(segIndex)}$EMPTYSTR", preNumLen, ZEROStr)}$DOT$whichSegment"
      val fieldLayout = mutable.LinkedHashMap[String, Any]()
      var fields: Array[String] = null
      if (whichSegment == MSH) {
        fields = msgSegment.split(ESCAPE + delimiters(FIELD_DELIM))
        versionData = getVersionData(fields, templateData)
        fields(0) = delimiters(FIELD_DELIM)
      } else {
        if (msgSegment.length > 4) fields = msgSegment.substring(4, msgSegment.length).split(ESCAPE + delimiters(FIELD_DELIM))
        else fields = msgSegment.split(ESCAPE + delimiters(FIELD_DELIM))
      }
      val moveToUnknown = moveUnknown(fieldLayout, unknownKey, _: String)
      if (valid(fields)) {
        fields.view.zipWithIndex foreach { case (field, fieldIndex) =>
          val fieldKey = s"$whichSegment$DOT${inc(fieldIndex)}"
          segment = segmentMapping(fieldKey)._1
          val fieldMapping = segment.mapping
          if (!(whichSegment == MSH && inc(fieldIndex) == 2)) {
            val multiFields = if (whichSegment == OBX_SEG && inc(fieldIndex) == 5) Array(field) else field.split(ESCAPE + delimiters(REPTN_DELIM), -1)
            val fieldList = new mutable.ListBuffer[Any]
            multiFields foreach { fieldRepeatItem =>
              breakable {
                val components = fieldRepeatItem.split(ESCAPE + delimiters(CMPNT_DELIM), -1)
                if (components.length > 1) {
                  val componentLayout = new mutable.LinkedHashMap[String, Any]()
                  var componentMapping = EMPTYSTR
                  components.view.zipWithIndex foreach { case (component, componentIndex) =>
                    val componentIndexKey = s"$fieldKey$DOT${inc(componentIndex)}"
                    segment = segmentMapping(componentIndexKey)._1
                    componentMapping = segment.mapping
                    var subComponents: Array[String] = EMPTY
                    if (segment.realignColStatus) {
                      segment.realignColOption match {
                        case MERGE =>
                          if (segment.realignColValue == EMPTYSTR) segment.realignColValue = component
                          else segment.realignColValue = segment.realignColValue + component
                        case MOVE =>
                          val subCompRep = fieldRepeatItem.replace(delimiters(CMPNT_DELIM), delimiters(SUBCMPNT_DELIM))
                          componentMapping = segment.realignCompName
                          subComponents = subCompRep.split(ESCAPE + delimiters(SUBCMPNT_DELIM), -1)
                        case _ =>
                      }
                    } else subComponents = component.split(ESCAPE + delimiters(SUBCMPNT_DELIM), -1)
                    if (subComponents.nonEmpty && subComponents.length > 1) {
                      val subComponentLayout = new mutable.LinkedHashMap[String, Any]()
                      subComponents.view.zipWithIndex foreach { case (subComponent, subComponentIndex) =>
                        val tempSegments = segmentMapping(s"$componentIndexKey$DOT${inc(subComponentIndex)}")._1
                        if (tempSegments.mapping == UNKNOWN) moveToUnknown(subComponent)
                        else subComponentLayout += tempSegments.mapping -> getOrNone(subComponent)
                      }
                      if (subComponentLayout nonEmpty) componentLayout += componentMapping -> subComponentLayout
                      if (segment.realignColStatus && segment.realignColOption == MOVE) {
                        if (fieldLayout isDefinedAt fieldMapping) {
                          val fieldList = new mutable.ListBuffer[Any]
                          fieldLayout(fieldMapping) match {
                            case _: mutable.ListBuffer[Any] =>
                              fieldLayout(fieldMapping).asInstanceOf[mutable.ListBuffer[Any]].foreach(fieldList += _)
                            case _ =>
                              fieldList += fieldLayout(fieldMapping)
                          }
                          fieldList += componentLayout
                          fieldLayout update(fieldMapping, fieldList)
                        } else fieldLayout += fieldMapping -> componentLayout
                        break
                      }
                    } else {
                      if (componentMapping == UNKNOWN) moveToUnknown(component)
                      else componentLayout += componentMapping -> getOrNone(component)
                    }
                  }
                  if (segment.realignColStatus) {
                    if (fieldMapping == UNKNOWN) moveToUnknown(segment.realignColValue)
                    else fieldLayout += fieldMapping -> componentLayout
                    segment.realignColValue = EMPTYSTR
                  } else {
                    if (multiFields.length > 1) fieldList += componentLayout
                    else if (componentLayout nonEmpty) fieldLayout += fieldMapping -> componentLayout
                  }
                }
                else {
                  if (fieldMapping == UNKNOWN) moveToUnknown(field)
                  else fieldLayout += fieldMapping -> getOrNone(field)
                }
              }
            }
            if (fieldList nonEmpty) {
              fieldLayout += fieldMapping -> fieldList
            }
          } else {
            segment = segmentMapping(fieldKey)._1
            if (segment.mapping == UNKNOWN) moveToUnknown(field)
            else fieldLayout += segment.mapping -> getOrNone(field)
          }
        }
      }
      dataLayout += segmentIndex -> fieldLayout
    }
    missingMappings.unknownMappings match {
      case null =>
        HL7Parsed(dataLayout, versionData.controlId.substring(0, versionData.controlId.indexOf("_")),
          versionData.hl7Version)
      case _ =>
        HL7Parsed(dataLayout, versionData.controlId.substring(0, versionData.controlId.indexOf("_")),
          versionData.hl7Version,
          s"Template Don't have mappings for ${missingMappings.unknownMappings.mkString(s"$COLON$COLON")} &" +
            s" Source System Version ${versionData.controlId.substring(0, versionData.controlId.indexOf("_"))}-${versionData.hl7Version}")
    }
  }

  private def moveUnknown(layout: mapType, mapping: String, data: String): Unit = {
    addUnknownEntry(mapping, layout)
    handleUnKnown(mapping, data, layout)
  }

  private def handleUnKnown(mapping: String, data: String, underlying: mapType): Unit = {
    if (underlying isDefinedAt mapping) {
      if (underlying(mapping) == EMPTYSTR) underlying update(mapping, data)
      else underlying update(mapping, s"${underlying(mapping)}$caret$data")
    }
  }

  private def addUnknownEntry(unknown: String, data: mapType): Unit = {
    if (!(data isDefinedAt unknown)) data += unknown -> EMPTYSTR
  }

  private def unknownKey(segment: String): String = s"${segment.toLowerCase}_$UNKNOWN"

  private def unknownKey: String = UNKNOWN

  private def inc(v: Int, step: Int = 1) = v + step

  private def getVersionData(fieldList: Array[String], templateData: Map[String, Map[String, Array[String]]]): VersionData = {
    val hl7Version = fieldList(11)
    val controlId = fieldList(9)
    require((controlId != EMPTYSTR) && (controlId contains underScore), s"Invalid Control Id $controlId")
    val Match = matcher(controlId, _: String)
    val sourceSystem = Match match {
      case mt6 if mt6(MT6_) =>
        if (hl7Version == HL7_2_4) templateData(hl7MT6_2_4Map)
        else if (hl7Version == HL7_2_5_1) templateData(hl7MT6_2_5_1Map)
        else if (hl7Version == HL7_2_5) templateData(hl7MT6_2_5Map)
        else NONE
      case mt if mt(MT_) =>
        if (hl7Version == HL7_2_1) templateData(hl7MT_2_1Map)
        else if (hl7Version == HL7_2_2) templateData(hl7MT_2_2Map)
        else if (hl7Version == HL7_2_4) templateData(hl7MT_2_4Map)
        else if (hl7Version == HL7_2_5) templateData(hl7MT_2_5Map)
        else NONE
      case epic if epic(EPIC_) =>
        if (hl7Version == HL7_2_1) templateData(hl7Epic_2_1Map)
        else if (hl7Version == HL7_2_3) templateData(hl7Epic_2_3Map)
        else if (hl7Version == HL7_2_3_1) templateData(hl7Epic_2_3_1Map)
        else NONE
      case ecw if ecw(ECW_) => templateData(hl7eCWMap)
      case ng if ng(NG_) => templateData(hl7NextGenMap)
      case ip if ip(IP_) => templateData(hl7IpeopleMap)
      case _ => NONE
    }

    def facility = () => fieldList(3)

    VersionData(controlId, hl7Version, tryAndReturnDefaultValue[String](facility, EMPTYSTR), sourceSystem, templateData(hl7StandardMap), templateData(hl7MapAlignXWalk), templateData(hl7FacilityMap))
  }

  private def matcher(in: String, seq: String) = in != null & in.startsWith(seq)


  private def getMapping(segmentIndex: String, controlVersion: String, facilityControlVersion: String, srcSystemMapping: Map[String, Array[String]]
                         , standardMapping: Map[String, Array[String]], realignment: Map[String, Array[String]], facilityOverRides: Map[String, Array[String]])
                        (controlId: String, missingMappings: TemplateUnknownMapping): (Segment, TemplateUnknownMapping) = {
    val segment = new Segment
    var mappedColumnData = EMPTYSTR
    try {
      val realignColumnValues = lookUp(realignment, s"$controlVersion$segmentIndex")
      if (realignColumnValues.nonEmpty) {
        segment.realignColStatus = true
        segment.realignFieldName = realignColumnValues(7)
        segment.realignCompName = realignColumnValues(8)
        segment.realignSubCompName = realignColumnValues(9)
        if (mappingExist(9, realignColumnValues) && nonEmpty(realignColumnValues(9))) mappedColumnData = realignColumnValues(9)
        else if (mappingExist(8, realignColumnValues) && nonEmpty(realignColumnValues(8))) mappedColumnData = realignColumnValues(8)
        else if (mappingExist(7, realignColumnValues) && nonEmpty(realignColumnValues(7))) mappedColumnData = realignColumnValues(7)
        segment.realignColOption = realignColumnValues(10)
        segment.mapping = mappedColumnData
        (segment, missingMappings)
      } else {
        segment.realignColStatus = false
        val standardMappedValues = lookUp(standardMapping, segmentIndex)
        val srcSystemValues = lookUp(srcSystemMapping, segmentIndex)
        val facilityOverRideValues = lookUp(facilityOverRides, s"$facilityControlVersion$segmentIndex")
        mappedColumnData = EMPTYSTR
        if (facilityOverRideValues nonEmpty) {
          if (mappingExist(2, facilityOverRideValues) && nonEmpty(facilityOverRideValues(2))) mappedColumnData = facilityOverRideValues(2)
          else if (mappingExist(1, facilityOverRideValues) && nonEmpty(facilityOverRideValues(1))) mappedColumnData = facilityOverRideValues(1)
          else if (mappingExist(0, facilityOverRideValues) && nonEmpty(facilityOverRideValues(0))) mappedColumnData = facilityOverRideValues(0)
        }
        else if (srcSystemValues nonEmpty) {
          if (mappingExist(2, srcSystemValues) && nonEmpty(srcSystemValues(2))) mappedColumnData = srcSystemValues(2)
          else if (mappingExist(1, srcSystemValues) && nonEmpty(srcSystemValues(1))) mappedColumnData = srcSystemValues(1)
          else if (mappingExist(0, srcSystemValues) && nonEmpty(srcSystemValues(0))) mappedColumnData = srcSystemValues(0)
        }
        else if (standardMappedValues nonEmpty) {
          if (mappingExist(5, standardMappedValues) && nonEmpty(standardMappedValues(5))) mappedColumnData = standardMappedValues(5)
          else if (mappingExist(4, standardMappedValues) && nonEmpty(standardMappedValues(4))) mappedColumnData = standardMappedValues(4)
          else if (mappingExist(3, standardMappedValues) && nonEmpty(standardMappedValues(3))) mappedColumnData = standardMappedValues(3)
          else if (mappingExist(2, standardMappedValues) && nonEmpty(standardMappedValues(2))) mappedColumnData = standardMappedValues(2)
          else if (mappingExist(1, standardMappedValues) && nonEmpty(standardMappedValues(1))) mappedColumnData = standardMappedValues(1)
          else if (mappingExist(0, standardMappedValues) && nonEmpty(standardMappedValues(0))) mappedColumnData = standardMappedValues(0)
        }
        if (mappedColumnData == EMPTYSTR || mappedColumnData.contains(DOT) ||
          (mappedColumnData.length > 2 && mappedColumnData.substring(3) == segmentIndex.substring(3))) {
          mappedColumnData = UNKNOWN
          if (missingMappings.unknownMappings == null) {
            missingMappings.unknownMappings = new mutable.HashSet[String]
          }
          missingMappings.unknownMappings += segmentIndex
        }
        segment.mapping = mappedColumnData
        (segment, missingMappings)
      }
    } catch {
      case t: Throwable =>
        throw new InvalidTemplateFormatException(s"Template has invalid Format for $segmentIndex & Source System Version $controlVersion " +
          s"& Msg Control Id $controlId . Cannot Apply Template Schema. Correct templates", t)
    }
  }

  private def mkCopy(data: mapType): mapType = {
    val copy = data ++: new mutable.LinkedHashMap[String, Any]
    filterNone(copy)
    copy
  }

  private def filterNone(data: mapType): Unit = {
    def remove(k: String): Unit = data remove k

    data.foreach({ case (k, v) =>
      v match {
        case None => remove(k)
        case map: mapType => filterNone(map)
        case list: listType => list.foreach(filterNone)
        case _ =>
      }
    })
  }

  private def nonEmpty(key: String) = key != null & key != EMPTYSTR

  private def getOrNone(key: String) = if (nonEmpty(key)) key else None

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