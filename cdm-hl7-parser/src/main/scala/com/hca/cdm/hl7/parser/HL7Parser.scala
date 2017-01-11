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
import org.apache.commons.lang3.{StringUtils => sutil}
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
class HL7Parser(val msgType: HL7, private val templateData: Map[String, Map[String, Array[String]]], private val reassignMeta: Map[String, String],
                private val reassignMapping: Map[String, Map[String, String]], private val reassignStruct: Map[String, mutable.Map[String, Any]]) extends Logg with Serializable {

  private lazy val toJson = new ObjectMapper().registerModule(DefaultScalaModule).writer.writeValueAsString(_)
  private lazy val EMPTY = Array.empty[String]
  private lazy val MAP = Map.empty[String, Array[String]]
  private val metrics = new TrieMap[String, Long]
  private val hl7 = msgType.toString
  HL7State.values.foreach(state => metrics += s"$hl7$COLON${state.toString}" -> 0L)
  outStream.println(
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
  @throws(classOf[TemplateInfoException])
  @throws(classOf[InvalidTemplateFormatException])
  def transformHL7(hl7Message: String, pre_num_len: Int = 4, segment_code_len: Int = 3, index_num_len: Int = 3): HL7TransRec = {
    require(hl7Message != null && !hl7Message.isEmpty, s"Error Nothing to parse $hl7Message")
    assume(isHL7(hl7Message), s"Not a Valid HL7. Check with Facility :: $hl7Message")
    val delim = if (hl7Message contains "\r\n") "\r\n" else "\n"
    Try(transform(hl7Message split delim, pre_num_len, segment_code_len, index_num_len)) match {
      case Success(parsed) =>
        handleCommonSegments(parsed.data)
        val meta = msgMeta(parsed.data)
        metRequirement(meta) match {
          case true =>
            if (msgHasmultiMSH(parsed.data)) throw new InvalidHl7FormatException(s"Message has Multiple MSH Segments. This is not Expected.")
            checkForReassignments(parsed)
            Try(toJson(parsed.data)) match {
              case Success(json) =>
                updateMetrics(PROCESSED)
                HL7TransRec(Left((json, parsed.data, meta)))
              case Failure(t) =>
                error(s"Transforming to Json Failed for HL7 ${t.getMessage}", t)
                updateMetrics(FAILED)
                HL7TransRec(Right(t))
            }
          case _ =>
            updateMetrics(REJECTED)
            throw new NotValidHl7Exception(invalidHl7)
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

  private def checkForReassignments(hL7Parsed: HL7Parsed): Unit = {
    if (!((reassignMeta isDefinedAt hL7Parsed.srcVersion) && (reassignMeta isDefinedAt hL7Parsed.sourceSystem))) return
    hL7Parsed.data.foreach(node => {
      if (reassignMeta isDefinedAt node._1.substring(node._1.indexOf(DOT) + 1)) {
        val versionSegment = s"${hL7Parsed.sourceSystem}${hL7Parsed.srcVersion}${node._1.substring(node._1.indexOf(DOT) + 1)}"
        if ((reassignStruct isDefinedAt versionSegment) && (reassignMapping isDefinedAt versionSegment)) {
          val structToReassign = structCopy(reassignStruct(versionSegment))
          val nodeToUpdate = node._2.asInstanceOf[mapType]
          reassign(reassignMapping(versionSegment), nodeToUpdate, structToReassign)
          var index = 0
          reassignMapping(versionSegment).foreach(reassign => {
            if (nodeToUpdate isDefinedAt reassign._2) nodeToUpdate update(reassign._2, structToReassign(reassign._2))
            index = inc(index)
            if (index == 1 && (nodeToUpdate isDefinedAt reassign._1)) nodeToUpdate update(reassign._1, EMPTYSTR)
          })
        }
      }
    })
  }

  private def structCopy(struct: mutable.Map[String, Any]): mutable.LinkedHashMap[String, Any] = {
    val temp = new mutable.LinkedHashMap[String, Any]()
    struct.foreach({ case (k, v) =>
      val struct = v match {
        case map: mutable.LinkedHashMap[String, String] =>
          val temp1 = new mutable.LinkedHashMap[String, String]()
          map.foreach({ case (k1, v1) => temp1 += k1 -> EMPTYSTR })
          temp1
        case s: String => EMPTYSTR
      }
      temp += k -> struct
    })
    temp
  }

  private def reassign(mapping: Map[String, String], data: mapType, structToReassign: mutable.Map[String, Any]): Unit = {
    mapping.foreach { case (k, v) =>
      if ((data isDefinedAt k) && (structToReassign isDefinedAt v)) {
        data(k) match {
          case map: mapType =>
            structToReassign(v) = swapData(map.asInstanceOf[mutable.Map[String, String]], structToReassign(v))
          case list: listType =>
            list.foreach(map => structToReassign(v) = swapData(map.asInstanceOf[mutable.Map[String, String]], structToReassign(v)))
          case s: String => structToReassign update(v, s)
        }
      }
    }
  }

  private def swapData(source: mutable.Map[String, String], dest: Any): Any = {
    dest match {
      case map: mutable.LinkedHashMap[String, String] =>
        var index = 0
        source.foreach { case (k, v) => map.view.zipWithIndex.foreach(x => if (x._2 == index) map update(x._1._1, v))
          index = inc(index)
        }
        map
      case s: String =>
        source.values mkString repeat
    }
  }

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

  private case class VersionData(var controlId: String = EMPTYSTR, var hl7Version: String = EMPTYSTR, var mappedIndex: Map[String, Array[String]] = MAP,
                                 var standardIndex: Map[String, Array[String]] = MAP, var realignIndex: Map[String, Array[String]] = MAP,
                                 var finalIndex: Map[String, Array[String]] = MAP)

  private case class HL7Parsed(data: mapType, sourceSystem: String, srcVersion: String)

  private def transform(rawSplit: Array[String], preNumLen: Int, segCodeLen: Int, indexNumLen: Int) = {
    val dataLayout = new mutable.LinkedHashMap[String, Any]
    dataLayout += commonNodeStr -> commonNode.clone().transform((k, v) => if (v ne EMPTYSTR) EMPTYSTR else v)
    val delimiters = new mutable.HashMap[String, String]()
    if (!(rawSplit(0) startsWith MSH)) throw new InvalidHl7FormatException("Expecting First Segment in HL7 as MSH but found :: " + rawSplit(0))
    delimiters.put(FIELD_DELIM, if (rawSplit(0).charAt(3) + EMPTYSTR == "|") "|" else (rawSplit(0).charAt(3) + EMPTYSTR).trim)
    delimiters.put(CMPNT_DELIM, if (rawSplit(0).charAt(4) + EMPTYSTR == "^") "^" else (rawSplit(0).charAt(4) + EMPTYSTR).trim)
    delimiters.put(REPTN_DELIM, (rawSplit(0).charAt(5) + EMPTYSTR).trim)
    delimiters.put(ESC_DELIM, if (rawSplit(0).charAt(6) + EMPTYSTR == "\\") "\\" else (rawSplit(0).charAt(6) + EMPTYSTR).trim)
    delimiters.put(SUBCMPNT_DELIM, (rawSplit(0).charAt(7) + EMPTYSTR).trim)
    delimiters.put(TRUNC_DELIM, if (rawSplit(0).charAt(8) + EMPTYSTR != "|") (rawSplit(0).charAt(8) + EMPTYSTR).trim else "//")
    var realignColStatus = false
    var segments: Segments = new Segments
    var realignVal = EMPTYSTR
    var versionData: VersionData = null
    val segmentsMapping = mapSegments(segments, _: String, _: Int, _: Int, versionData.controlId.substring(0, versionData.controlId.indexOf("_")) + "_" + versionData.hl7Version + "_", versionData.mappedIndex,
      versionData.standardIndex, versionData.realignIndex, versionData.finalIndex)(versionData.controlId)
    var i = 0
    rawSplit.foreach(msgSegment => {
      i = inc(i)
      val whichSegment = msgSegment substring(0, 3)
      val segmentIndex = sutil.leftPad(i + EMPTYSTR, preNumLen, ZEROStr) + DOT + whichSegment
      val componentLayout = mutable.LinkedHashMap[String, Any]()
      var fields: Array[String] = null
      whichSegment == MSH match {
        case true => fields = msgSegment.split("\\" + delimiters(FIELD_DELIM))
          versionData = getVersionData(fields, templateData)
          fields(0) = delimiters(FIELD_DELIM)
        case _ => if (msgSegment.length > 4) fields = msgSegment.substring(4, msgSegment.length).split("\\" + delimiters(FIELD_DELIM))
        else fields = msgSegment.split("\\" + delimiters(FIELD_DELIM))
      }
      fields.nonEmpty match {
        case true =>
          var j = 0
          fields.foreach(field => {
            j = inc(j)
            val componentIndex = whichSegment + DOT + sutil.leftPad(j + EMPTYSTR, indexNumLen, ZEROStr)
            segments = segmentsMapping(componentIndex, segCodeLen, indexNumLen)
            val componentData = segments.componentData
            !(whichSegment == MSH && j == 2) match {
              case true =>
                val multiFields = if (whichSegment == OBX_SEG && j == 5) Array(field) else field.split("\\" + delimiters(REPTN_DELIM), -1)
                val componentList = new mutable.ListBuffer[Any]
                multiFields.foreach(fieldRepeatItem => {
                  val subComponent = fieldRepeatItem.split("\\" + delimiters(CMPNT_DELIM), -1)
                  if (subComponent.length > 1) {
                    val subComponentLayout = new mutable.LinkedHashMap[String, Any]()
                    var subComponentData = EMPTYSTR
                    var k = 0
                    subComponent.foreach(subComp => {
                      k = inc(k)
                      val subComponentIndex = componentIndex + DOT + sutil.leftPad(k + EMPTYSTR, indexNumLen, ZEROStr)
                      segments = segmentsMapping(subComponentIndex, segCodeLen, indexNumLen)
                      subComponentData = segments.componentData
                      var subSubComponent: Array[String] = EMPTY
                      realignColStatus = segments.realignColStatus
                      realignColStatus match {
                        case true => segments.realignColOption match {
                          case MERGE =>
                            if (realignVal == EMPTYSTR) realignVal = subComp
                            else realignVal = realignVal + subComp
                          case MOVE =>
                            val subCompRep = fieldRepeatItem.replace(delimiters(CMPNT_DELIM), delimiters(SUBCMPNT_DELIM))
                            subComponentData = generateMapIndex(subComponentIndex, indexNumLen) + DOT + segments.realignCompName
                            subSubComponent = subCompRep.split("\\" + delimiters(SUBCMPNT_DELIM), -1)
                          case _ =>
                        }
                        case _ => subSubComponent = subComp.split("\\" + delimiters(SUBCMPNT_DELIM), -1)
                      }
                      subSubComponent.nonEmpty & subSubComponent.length > 1 match {
                        case true =>
                          val subSubComponentLayout = new mutable.LinkedHashMap[String, Any]()
                          var l = 0
                          subSubComponent.foreach(msgSubSubComponent => {
                            l = inc(l)
                            segments = segmentsMapping(subComponentIndex + DOT + sutil.leftPad(l + EMPTYSTR, indexNumLen, ZEROStr), segCodeLen, indexNumLen)
                            subSubComponentLayout += segments.componentData -> msgSubSubComponent
                          })
                          subComponentLayout += subComponentData -> subSubComponentLayout
                          if (realignColStatus & segments.realignColOption == MOVE) {
                            componentLayout += componentData -> subComponentLayout
                            break
                          }
                        case _ => subComponentLayout += subComponentData -> subComp
                      }
                    })
                    realignColStatus match {
                      case true =>
                        componentLayout += componentData -> realignVal
                        segments.realignColValue = EMPTYSTR
                        realignColStatus = false
                        realignVal = EMPTYSTR
                      case _ => if (multiFields.length > 1) componentList += subComponentLayout
                      else componentLayout += componentData -> subComponentLayout
                    }
                  }
                  else componentLayout += componentData -> field
                })
                if (multiFields.length > 1) componentLayout += componentData -> componentList
              case _ => componentLayout += segmentsMapping(componentIndex, segCodeLen, indexNumLen).componentData -> field
            }
          })
        case _ =>
      }
      dataLayout += segmentIndex -> componentLayout
    })
    HL7Parsed(dataLayout, versionData.controlId.substring(0, versionData.controlId.indexOf("_")), versionData.hl7Version)
  }


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
    VersionData(controlId, hl7Version, mapped_index, templateData(hl7StandardMap), templateData(hl7MapAlignXWalk), templateData(hl7FinalMap))
  }

  private def matcher(in: String, seq: String) = in != null & in.contains(seq)


  private def mapSegments(segment: Segments, mappingElement: String, segCodeLen: Int, indexLen: Int, controlVersion: String, mappedIndex: Map[String, Array[String]]
                          , standardIndex: Map[String, Array[String]], realignIndex: Map[String, Array[String]], finalIndex: Map[String, Array[String]])(controlId: String): Segments = {
    val segments = resetSegment(segment)
    var segmentIndex = EMPTYSTR
    var mappedColumnData = EMPTYSTR
    if (mappingElement.split("\\.").length > 2) {
      val firstDelimPos = mappingElement.indexOf(DOT) + 1
      val secondDelimPos = mappingElement.lastIndexOf(DOT) + 1
      segmentIndex = mappingElement.substring(0, segCodeLen + 1) + mappingElement.substring(firstDelimPos + 1, firstDelimPos + indexLen).
        replaceFirst(REPEAT_ZERO_STAR, EMPTYSTR) + DOT + mappingElement.substring(secondDelimPos + 1, secondDelimPos + indexLen).
        replaceFirst(REPEAT_ZERO_STAR, EMPTYSTR)
    } else segmentIndex = mappingElement.substring(0, segCodeLen + 1) + mappingElement.substring(mappingElement.indexOf(DOT) + 1,
      mappingElement.length).replaceFirst(REPEAT_ZERO_STAR, EMPTYSTR)
    try {
      val realignColumnValues = lookUp(realignIndex, controlVersion + segmentIndex)
      realignColumnValues.nonEmpty match {
        case true =>
          segments.realignColStatus = true
          segmentIndex = realignColumnValues(6)
          segments.realignFieldName = realignColumnValues(7)
          segments.realignCompName = realignColumnValues(8)
          segments.realignSubCompName = realignColumnValues(9)
          if (mappingExist(7, realignColumnValues) && nonEmpty(realignColumnValues(7))) mappedColumnData = realignColumnValues(7)
          if (mappingExist(8, realignColumnValues) && nonEmpty(realignColumnValues(8))) mappedColumnData = realignColumnValues(8)
          if (mappingExist(9, realignColumnValues) && nonEmpty(realignColumnValues(9))) mappedColumnData = realignColumnValues(9)
          segments.realignColOption = realignColumnValues(10)
          Segments(generateMapIndex(segmentIndex, indexLen) + DOT + mappedColumnData, segments.realignColStatus, EMPTYSTR, segments.realignFieldName,
            segments.realignCompName, segments.realignSubCompName, segments.realignColOption)
        case _ =>
          segments.realignColStatus = false
          val standardMappedColumnValues = lookUp(standardIndex, segmentIndex)
          val mappedColumnValues = lookUp(mappedIndex, segmentIndex)
          val mappedFinalResultValues = lookUp(finalIndex, segmentIndex)
          mappedColumnData = segmentIndex
          standardMappedColumnValues.nonEmpty match {
            case true =>
              if (mappingExist(2, standardMappedColumnValues) && nonEmpty(standardMappedColumnValues(2))) mappedColumnData = standardMappedColumnValues(2)
              else if (mappingExist(1, standardMappedColumnValues) && nonEmpty(standardMappedColumnValues(1))) mappedColumnData = standardMappedColumnValues(1)
              else if (mappingExist(0, standardMappedColumnValues) && nonEmpty(standardMappedColumnValues(0))) mappedColumnData = standardMappedColumnValues(0)
            case _ =>
          }
          mappedColumnValues.nonEmpty match {
            case true =>
              if (mappingExist(2, mappedColumnValues) && nonEmpty(mappedColumnValues(2))) mappedColumnData = mappedColumnValues(2)
              else if (mappingExist(1, mappedColumnValues) && nonEmpty(mappedColumnValues(1))) mappedColumnData = mappedColumnValues(1)
              else if (mappingExist(0, mappedColumnValues) && nonEmpty(mappedColumnValues(0))) mappedColumnData = mappedColumnValues(0)
            case _ =>
          }
          mappedFinalResultValues.nonEmpty match {
            case true => if (mappingExist(6, mappedFinalResultValues) && nonEmpty(mappedFinalResultValues(6))) mappedColumnData = mappedFinalResultValues(6)
            else if (mappingExist(5, mappedFinalResultValues) && nonEmpty(mappedFinalResultValues(5))) mappedColumnData = mappedFinalResultValues(5)
            else if (mappingExist(4, mappedFinalResultValues) && nonEmpty(mappedFinalResultValues(4))) mappedColumnData = mappedFinalResultValues(4)
            case _ =>
          }
          if (mappedColumnData == EMPTYSTR || mappedColumnData.contains(DOT) || mappedColumnData.substring(3) == mappingElement.substring(3)) throw new TemplateInfoException(s"Template Don't have mapping for $mappingElement & Source System Version $controlVersion & Msg Control Id $controlId  ")
          segments.componentData = mappingElement.substring(mappingElement.lastIndexOf(DOT) + 1, mappingElement.lastIndexOf(DOT) + 1 + indexLen) + DOT + mappedColumnData
          segments
      }
    } catch {
      case noTemplateMapping: TemplateInfoException =>
        throw noTemplateMapping
      case t: Throwable =>
        t.printStackTrace()
        throw new InvalidTemplateFormatException(s"Template has invalid Format for $mappingElement & Source System Version $controlVersion & Msg Control Id $controlId . Cannot Apply Template Schema. Correct templates", t)
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

  private def generateMapIndex(segmentIndex: String, indexLen: Int): String = {
    val split = segmentIndex.split("\\.")
    split nonEmpty match {
      case true => sutil.leftPad(split(split.length - 1), indexLen, ZEROStr)
      case _ => segmentIndex
    }
  }

  override def toString: String = s"HL7Parser($hl7, $templateData, $metrics, )"
}

object HL7Parser {

  def apply(msgType: HL7, templateMappings: Map[String, Map[String, Array[String]]], reassignMeta: Map[String, String],
            reassignMapping: Map[String, Map[String, String]], reassignStruct: Map[String, mutable.Map[String, Any]]): HL7Parser =
    new HL7Parser(msgType, templateMappings, reassignMeta, reassignMapping, reassignStruct)

}