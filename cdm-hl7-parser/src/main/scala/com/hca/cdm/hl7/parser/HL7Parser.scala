package com.hca.cdm.hl7.parser

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.hca.cdm
import com.hca.cdm._
import com.hca.cdm.exception.CdmException
import com.hca.cdm.hl7.audit.AuditConstants._
import com.hca.cdm.hl7.audit._
import com.hca.cdm.hl7.constants.HL7Constants._
import com.hca.cdm.hl7.constants.HL7Types.HL7
import com.hca.cdm.hl7.constants.{FileMappings => files}
import com.hca.cdm.hl7.model.HL7State._
import com.hca.cdm.hl7.model.{HL7TransRec, _}
import com.hca.cdm.hl7.validation.NotValidHl7Exception
import com.hca.cdm.hl7.validation.ValidationUtil.{isValidMsg => metRequirement}
import com.hca.cdm.log.Logg
import org.apache.commons.lang.StringUtils
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.util.control.Breaks._
import scala.util.{Failure, Success, Try}

class HL7Parser(val msgType: HL7, private val templateData: Map[String, Map[String, Array[String]]]) extends Logg with Serializable {

  private lazy val toJson = new ObjectMapper().registerModule(DefaultScalaModule).writer.writeValueAsString(_)
  private lazy val EMPTY = Array.empty[String]
  private lazy val MAP = Map.empty[String, Array[String]]
  private val metrics = new TrieMap[String, Long]
  private val hl7 = msgType.toString
  HL7State.values.foreach(state => metrics += hl7 + COLON + state.toString -> 0L)
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
  @throws(classOf[CdmException])
  def transformHL7(hl7Message: String, pre_num_len: Int = 4, segment_code_len: Int = 3, index_num_len: Int = 3): HL7TransRec = {
    require(hl7Message != null && !hl7Message.isEmpty, "Error Nothing to parse " + hl7Message)
    assume(isHL7(hl7Message), "Not a Valid HL7. Check with Facility :: " + hl7Message)
    val delim = if (hl7Message contains "\r\n") "\r\n" else "\n"
    Try(transform(hl7Message split delim, pre_num_len, segment_code_len, index_num_len)) match {
      case Success(map) =>
        handleCommonSegments(map)
        val meta = msgMeta(map)
        metRequirement(meta) match {
          case true => Try(toJson(map)) match {
            case Success(json) =>
              updateMetrics(PROCESSED)
              HL7TransRec(Left((json, map, meta)))
            case Failure(t) =>
              error("Transforming to Json Failed for HL7 " + t.getMessage, t)
              updateMetrics(FAILED)
              HL7TransRec(Right(t))
          }
          case _ =>
            updateMetrics(REJECTED)
            throw new NotValidHl7Exception(invalidHl7)
        }
      case Failure(t) =>
        error(t.getMessage, t)
        updateMetrics(FAILED)
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

  private def updateMetrics(state: hl7State) = {
    val key = hl7 + COLON + state
    metrics get key match {
      case Some(stat) => metrics update(key, cdm.inc(stat))
      case _ => throw new DataModelException("Cannot Update Metrics Key not Found." + logIdent + " This should not Happen :: " + state)
    }
  }

  private def findReqSegment(data: mapType, reqSeg: String): String = {
    var segment = EMPTYSTR
    breakable {
      data.foreach(node => {
        if (node._1.substring(node._1.indexOf(".") + 1) == reqSeg) {
          segment = node._1
          break
        }
      })
    }
    segment
  }

  private def handleCommonSegments(data: mapType) = {
    val commonNode = data(commonNodeStr).asInstanceOf[mapType]
    handleIndexes(findReqSegment(data,MSH), data, commonNode)
    handleIndexes(findReqSegment(data,PID), data, commonNode)
  }

  private def handleIndexes(segIndex: String, map: mapType, commonNode: mapType) = {
    map.get(segIndex) match {
      case Some(node) =>
        val temp = node.asInstanceOf[mapType]
        commonNode.foreach(ele => {
          if (temp isDefinedAt ele._1) {
            temp.get(ele._1) match {
              case Some(str: String) => if (str != EMPTYSTR) commonNode update(ele._1, str)
              case Some(map: mapType) => map.get(ele._1) match {
                case Some(v: String) =>
                  commonNode update(ele._1, v)
                case Some(m: mapType) =>
                  m foreach { case (mk, mv) =>
                    if (mk.substring(mk.indexOf(".") + 1) == ele._1.substring(ele._1.indexOf(".") + 1)) {
                      mv match {
                        case str: String => if (str != EMPTYSTR) {
                          if (commonNode(ele._1) == EMPTYSTR) commonNode update(ele._1, str)
                          else commonNode update(ele._1, commonNode(ele._1) + repeat + str)
                        }
                        case _ =>
                      }
                    }
                  }
                case Some(list: listType) => list.foreach(map => {
                  map foreach { case (mk, mv) =>
                    if (mk.substring(mk.indexOf(".") + 1) == ele._1.substring(ele._1.indexOf(".") + 1)) {
                      mv match {
                        case str: String => if (str != EMPTYSTR) {
                          if (commonNode(ele._1) == EMPTYSTR) commonNode update(ele._1, str)
                          else commonNode update(ele._1, commonNode(ele._1) + repeat + str)
                        }
                        case _ =>
                      }
                    }
                  }
                })
                case _ =>
              }
              case Some(list: listType) => list.foreach(map => {
                map foreach { case (mk, mv) =>
                  if (mk.substring(mk.indexOf(".") + 1) == ele._1.substring(ele._1.indexOf(".") + 1)) {
                    mv match {
                      case str: String => if (str != EMPTYSTR) {
                        if (commonNode(ele._1) == EMPTYSTR) commonNode update(ele._1, str)
                        else commonNode update(ele._1, commonNode(ele._1) + repeat + str)
                      }
                    }
                  }
                }
              })
            }
          }
        })
      case _ =>
    }

  }


  private case class Segments(var strComponentEleData: String = EMPTYSTR, var realignColStatus: Boolean = false, var realignColValue: String = EMPTYSTR,
                              var realignFieldName: String = EMPTYSTR, var realignCompName: String = EMPTYSTR, var realignSubCompName: String = EMPTYSTR, var realignColOption: String = EMPTYSTR)

  private case class VersionData(var control_id: String = EMPTYSTR, var hl7_version: String = EMPTYSTR, var mapped_index: Map[String, Array[String]] = MAP,
                                 var standard_mapped_index: Map[String, Array[String]] = MAP, var realign_index: Map[String, Array[String]] = MAP,
                                 var final_mapped_index: Map[String, Array[String]] = MAP)

  private def transform(rawMessage: Array[String], preNumLen: Int, segCodeLen: Int, indexNumLen: Int) = {
    val mapSegment = new mutable.LinkedHashMap[String, Any]
    mapSegment += commonNodeStr -> commonNode.clone().transform((k, v) => if (v ne EMPTYSTR) EMPTYSTR else v)
    val msg_delims = new mutable.HashMap[String, String]()
    msg_delims.put(FIELD_DELIM, if (rawMessage(0).charAt(3) + EMPTYSTR == "|") "|" else (rawMessage(0).charAt(3) + EMPTYSTR).trim)
    msg_delims.put(CMPNT_DELIM, if (rawMessage(0).charAt(4) + EMPTYSTR == "^") "^" else (rawMessage(0).charAt(4) + EMPTYSTR).trim)
    msg_delims.put(REPTN_DELIM, (rawMessage(0).charAt(5) + EMPTYSTR).trim)
    msg_delims.put(ESC_DELIM, if (rawMessage(0).charAt(6) + EMPTYSTR == "\\") "\\" else (rawMessage(0).charAt(6) + EMPTYSTR).trim)
    msg_delims.put(SUBCMPNT_DELIM, (rawMessage(0).charAt(7) + EMPTYSTR).trim)
    msg_delims.put(TRUNC_DELIM, if (rawMessage(0).charAt(8) + EMPTYSTR != "|") (rawMessage(0).charAt(8) + EMPTYSTR).trim else "//")
    var realignColStatus = false
    var segments: Segments = null
    var realignVal = EMPTYSTR
    var versionData: VersionData = null
    val segmentsMapping = mapSegments(_: String, _: Int, _: Int, versionData.control_id, versionData.hl7_version, versionData.mapped_index,
      versionData.standard_mapped_index, versionData.realign_index, versionData.final_mapped_index)
    var i = 0
    try {
      rawMessage.foreach(msgSegment => {
        i = inc(i)
        val segment_type = msgSegment.substring(0, 3)
        val segment_index = StringUtils.leftPad(i + EMPTYSTR, preNumLen, ZEROStr) + "." + segment_type
        val mapComponent = mutable.LinkedHashMap[String, Any]()
        var msg_field_list: Array[String] = null
        segment_type == MSH match {
          case true => msg_field_list = msgSegment.split("\\" + msg_delims(FIELD_DELIM))
            versionData = getVersionData(msg_field_list, templateData)
            msg_field_list(0) = msg_delims(FIELD_DELIM)
          case _ => if (msgSegment.length > 4) msg_field_list = msgSegment.substring(4, msgSegment.length).split("\\" + msg_delims(FIELD_DELIM))
          else msg_field_list = msgSegment.split("\\" + msg_delims(FIELD_DELIM))
        }
        msg_field_list.nonEmpty match {
          case true => var j = 0
            msg_field_list.foreach(field => {
              j = inc(j)
              val component_index = segment_type + "." + StringUtils.leftPad(j + EMPTYSTR, indexNumLen, ZEROStr)
              segments = segmentsMapping(component_index, segCodeLen, indexNumLen)
              val strComponentEleData = segments.strComponentEleData
              !(segment_type == MSH && j == 2) match {
                case true => val field_repeat_list = field.split("\\" + msg_delims(REPTN_DELIM))
                  val mapComponentList = new mutable.ListBuffer[Any]
                  breakable {
                    field_repeat_list.foreach(fieldRepeatItem => {
                      val subComponent = fieldRepeatItem.split("\\" + msg_delims(CMPNT_DELIM), -1)
                      if (subComponent.length > 1) {
                        val mapSubComponent = new mutable.LinkedHashMap[String, Any]()
                        var strSubComponentEleData = EMPTYSTR
                        var k = 0
                        subComponent.foreach(msgSubComp => {
                          k = inc(k)
                          val subcomponent_index = component_index + "." + StringUtils.leftPad(k + EMPTYSTR, indexNumLen, ZEROStr)
                          segments = segmentsMapping(subcomponent_index, segCodeLen, indexNumLen)
                          strSubComponentEleData = segments.strComponentEleData
                          var subSubComponent: Array[String] = EMPTY
                          realignColStatus = segments.realignColStatus
                          realignColStatus match {
                            case true => segments.realignColOption match {
                              case `MERGE` =>
                                if (realignVal == EMPTYSTR) realignVal = msgSubComp
                                else realignVal = realignVal + msgSubComp
                              case `MOVE` => val msgSubCompRep = fieldRepeatItem.replace(msg_delims(CMPNT_DELIM), msg_delims(SUBCMPNT_DELIM))
                                strSubComponentEleData = generateMapIndex(subcomponent_index, indexNumLen) + "." + segments.realignCompName
                                subSubComponent = msgSubCompRep.split("\\" + msg_delims(SUBCMPNT_DELIM), -1)
                              case _ =>
                            }
                            case _ => subSubComponent = msgSubComp.split("\\" + msg_delims(SUBCMPNT_DELIM), -1)
                          }
                          subSubComponent.nonEmpty & subSubComponent.length > 1 match {
                            case true =>
                              val mapSubSubComponent = new mutable.LinkedHashMap[String, Any]()
                              var l = 0
                              subSubComponent.foreach(msgSubSubComponent => {
                                l = inc(l)
                                segments = segmentsMapping(subcomponent_index + "." + StringUtils.leftPad(l + EMPTYSTR, indexNumLen, ZEROStr), segCodeLen, indexNumLen)
                                mapSubSubComponent += segments.strComponentEleData -> msgSubSubComponent
                              })
                              mapSubComponent += strSubComponentEleData -> mapSubSubComponent
                              if (realignColStatus & segments.realignColOption == MOVE) {
                                mapComponent += strComponentEleData -> mapSubComponent
                                break
                              }
                            case _ => mapSubComponent += strSubComponentEleData -> msgSubComp
                          }
                        })
                        realignColStatus match {
                          case true =>
                            mapComponent += strComponentEleData -> realignVal
                            segments.realignColValue = EMPTYSTR
                            realignColStatus = false
                            realignVal = EMPTYSTR
                          case _ => if (field_repeat_list.length > 1) mapComponentList += mapSubComponent
                          else mapComponent += strComponentEleData -> mapSubComponent
                        }
                      }
                      else mapComponent += strComponentEleData -> field
                    })
                  }
                  if (field_repeat_list.length > 1) mapComponent += strComponentEleData -> mapComponentList
                case _ => mapComponent += segmentsMapping(component_index, segCodeLen, indexNumLen).strComponentEleData -> field
              }
            })
          case _ =>
        }
        mapSegment += segment_index -> mapComponent
      })
      mapSegment
    } catch {
      case t: Throwable => throw new CdmException(t)
    }
  }


  private def inc(v: Int, step: Int = 1) = v + step

  private def getVersionData(msgFieldList: Array[String], templateData: Map[String, Map[String, Array[String]]]): VersionData = {
    val hl7_version = msgFieldList(11)
    val control_id = msgFieldList(9)
    val Match = matcher(control_id, _: String)
    val mapped_index = Match match {
      case mt_mt6 if mt_mt6(MT_) | mt_mt6(MT6_) =>
        if (hl7_version == HL7_2_1) templateData(files.hl7MEDITECH21Map)
        else if (hl7_version == HL7_2_4 | hl7_version == HL7_2_5_1) templateData(files.hl7MEDITECH24Map)
        else templateData(files.hl7StandardMap)
      case epic if epic(EPIC_) => templateData(files.hl7EpicMap)
      case ecw if ecw(ECW_) => templateData(files.hl7eCWMap)
      case ng if ng(NG_) => templateData(files.hl7NextGenMap)
      case ip if ip(IP_) => templateData(files.hl7IpeopleMap)
      case _ => MAP
    }
    VersionData(control_id, hl7_version, mapped_index, templateData(files.hl7StandardMap), templateData(files.hl7MapAlignXWalk), templateData(files.hl7FinalMap))
  }

  private def matcher(in: String, seq: String) = in != null & in.contains(seq)


  private def mapSegments(strMappingElement: String, segCodeLen: Int, indexNumLen: Int, control_id: String, hl7_version: String, mapped_index: Map[String, Array[String]]
                          , standard_mapped_index: Map[String, Array[String]], realign_index: Map[String, Array[String]], final_mapped_index: Map[String, Array[String]]): Segments = {
    val segments = new Segments
    var segindex = EMPTYSTR
    var mappedColumnData = EMPTYSTR
    if (strMappingElement.split("\\.").length > 2) {
      val firstDelimPos = strMappingElement.indexOf(".") + 1
      val secondDelimPos = strMappingElement.lastIndexOf(".") + 1
      segindex = strMappingElement.substring(0, segCodeLen + 1) + strMappingElement.substring(firstDelimPos + 1, firstDelimPos + indexNumLen).
        replaceFirst(REPEAT_ZERO_STAR, EMPTYSTR) + "." + strMappingElement.substring(secondDelimPos + 1, secondDelimPos + indexNumLen).
        replaceFirst(REPEAT_ZERO_STAR, EMPTYSTR)
    } else segindex = strMappingElement.substring(0, segCodeLen + 1) + strMappingElement.substring(strMappingElement.indexOf(".") + 1,
      strMappingElement.length).replaceFirst(REPEAT_ZERO_STAR, EMPTYSTR)
    try {
      val realignColumnValues = lookUp(realign_index, control_id.substring(0, control_id.indexOf("_")) + "_" + hl7_version + "_" + segindex)
      realignColumnValues.nonEmpty match {
        case true => segments.realignColStatus = true
          segindex = realignColumnValues(6)
          segments.realignFieldName = realignColumnValues(7)
          segments.realignCompName = realignColumnValues(8)
          segments.realignSubCompName = realignColumnValues(9)
          if (nonEmpty(realignColumnValues(7))) mappedColumnData = realignColumnValues(7)
          if (nonEmpty(realignColumnValues(8))) mappedColumnData = realignColumnValues(8)
          if (nonEmpty(realignColumnValues(9))) mappedColumnData = realignColumnValues(9)
          segments.realignColOption = realignColumnValues(10)
          Segments(generateMapIndex(segindex, indexNumLen) + "." + mappedColumnData, segments.realignColStatus, EMPTYSTR, segments.realignFieldName,
            segments.realignCompName, segments.realignSubCompName, segments.realignColOption)
        case _ => segments.realignColStatus = false
          val standardMappedColumnValues = lookUp(standard_mapped_index, segindex)
          val mappedColumnValues = lookUp(mapped_index, segindex)
          val mappedFinalResultValues = lookUp(final_mapped_index, segindex)
          mappedColumnData = segindex
          standardMappedColumnValues.nonEmpty match {
            case true =>
              if (nonEmpty(standardMappedColumnValues(2))) mappedColumnData = standardMappedColumnValues(2)
              else if (nonEmpty(standardMappedColumnValues(1))) mappedColumnData = standardMappedColumnValues(1)
              else if (nonEmpty(standardMappedColumnValues(0))) mappedColumnData = standardMappedColumnValues(0)
            case _ =>
          }
          mappedColumnValues.nonEmpty match {
            case true =>
              if (nonEmpty(mappedColumnValues(2))) mappedColumnData = mappedColumnValues(2)
              else if (nonEmpty(mappedColumnValues(1))) mappedColumnData = mappedColumnValues(1)
              else if (nonEmpty(mappedColumnValues(0))) mappedColumnData = mappedColumnValues(0)
            case _ =>
          }
          if (mappedColumnData == EMPTYSTR) mappedColumnData = NO_COLUMN_ASSIGNED
          mappedFinalResultValues.nonEmpty match {
            case true => if (nonEmpty(mappedFinalResultValues(6))) mappedColumnData = mappedFinalResultValues(6)
            else if (nonEmpty(mappedFinalResultValues(5))) mappedColumnData = mappedFinalResultValues(5)
            else if (nonEmpty(mappedFinalResultValues(4))) mappedColumnData = mappedFinalResultValues(4)
            case _ =>
          }
          segments.strComponentEleData = strMappingElement.substring(strMappingElement.lastIndexOf(".") + 1, strMappingElement.lastIndexOf(".") + 1 +
            indexNumLen) + "." + mappedColumnData
          segments
      }
    } catch {
      case t: Throwable => segments.strComponentEleData = strMappingElement + "." + mappedColumnData
        segments

    }
  }


  private def nonEmpty(key: String) = key != null & key != EMPTYSTR

  private def isHL7(message: String) = matcher(message, MSH)

  private def lookUp(store: Map[String, Array[String]], key: String) = {
    store.get(key) match {
      case Some(data) => data
      case _ => EMPTY
    }
  }

  private def generateMapIndex(segindex: String, indexNumLen: Int): String = {
    val indArry = segindex.split("\\.")
    indArry.nonEmpty match {
      case true => StringUtils.leftPad(indArry(indArry.length - 1), indexNumLen, ZEROStr)
      case _ =>
    }
    segindex
  }

  override def toString = s"HL7Parser($hl7, $templateData, $metrics, )"
}

object HL7Parser {

  def apply(msgType: HL7, templateMappings: Map[String, Map[String, Array[String]]]): HL7Parser = new HL7Parser(msgType, templateMappings)

}