package com.hca.cdm.hl7

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.hca.cdm._
import com.hca.cdm.exception.CdmException
import com.hca.cdm.hl7.audit.MSGMeta
import com.hca.cdm.hl7.constants.HL7Constants._
import com.hca.cdm.hl7.constants.HL7Types._
import com.hca.cdm.hl7.model.OutFormats.OutFormat
import com.hca.cdm.hl7.model.SegmentsState.SegState
import com.hca.cdm.utils.DateUtil.{currentTimeStamp => timeStamp}
import com.hca.cdm.utils.Filters.Conditions.{withName => matchCriteria}
import com.hca.cdm.utils.Filters.Expressions.{withName => relationWithNextFilter}
import com.hca.cdm.utils.Filters.FILTER
import scala.collection.mutable
import scala.io.Source
import scala.language.postfixOps
import scala.util.{Success, Try}

/**
  * Created by Devaraj Jonnadula on 8/22/2016.
  *
  * Package with Utilities for Loading Templates, Segments ...
  */
package object model {

  lazy val MSH_Segment = "0001.MSH"
  lazy val Message_Type_Segment = "009.msh_msg_type"
  lazy val Message_Control_Id = "001.message_code"
  lazy val sending_Facility = "004.msh_sending_facility"
  lazy val Msg_Type_Hier = Seq(MSH_Segment, Message_Type_Segment, Message_Control_Id)
  lazy val Observation_Col = "005.obx_observation_value"
  lazy val MSH_INDEX = "0001.MSH"
  lazy val commonNodeStr = "0000.COMN"
  lazy val OBX_SEG = "OBX"
  lazy val commonSegkey = "seg"
  lazy val notValidStr = "Not Valid Input"
  lazy val skippedStr = "SKIPPED"
  lazy val filteredStr = "FILTERED"
  lazy val timeStampKey = "etl_firstinsert_datetime"
  lazy val NA = "Not Applicable"
  private lazy val toJson = new ObjectMapper().registerModule(DefaultScalaModule).writer.writeValueAsString(_)
  private lazy val NONE = MSGMeta("no message control ID", "no time from message", EMPTYSTR, EMPTYSTR, EMPTYSTR, "no Sending Facility")
  lazy val repeat = "^"
  lazy val DOT = "."
  lazy val AMPERSAND = "&"
  lazy val EQUAL = "="
  lazy val segmentSequence = "segment_sequence"
  val commonNode = synchronized {
    val temp = new mutable.LinkedHashMap[String, String]
    val nodeEle = lookUpProp("common.elements") match {
      case EMPTYSTR => throw new CdmException("No Common Elements found. ")
      case valid => valid split("\\^", -1)
    }
    nodeEle.foreach(ele => temp += ele -> EMPTYSTR)
    temp
  }

  val MSHMappings = synchronized(commonSegmentMappings(lookUpProp("common.elements.msh.mappings")))
  val PIDMappings = synchronized(commonSegmentMappings(lookUpProp("common.elements.pid.mappings")))

  private def commonSegmentMappings(mappingData: String) = {
    val temp = new mutable.HashMap[String, Array[(String, String, String, String)]]
    val nodeEle = mappingData match {
      case EMPTYSTR => throw new CdmException("No Common Elements Sub Components Mappings found. ")
      case valid => valid split("\\^", -1)
    }
    nodeEle.foreach(ele => {
      val subComp = ele.split(",", -1)
      temp += subComp.head -> subComp.map(x => {
        if (x contains PIPE_DELIMITED) {
          val split = x split("\\" + PIPE_DELIMITED, -1)
          val temp = split(1)
          if (temp contains AMPERSAND) {
            val subCompSplit = temp split("\\" + AMPERSAND, -1)
            val kv = subCompSplit(1) split("\\" + EQUAL, -1)
            (split(0), subCompSplit(0), kv(0), kv(1))
          } else (split(0), temp, EMPTYSTR, EMPTYSTR)
        } else (x, EMPTYSTR, EMPTYSTR, EMPTYSTR)
      })

    })
    temp

  }

  object HL7State extends Enumeration {
    type hl7State = Value
    val PROCESSED = Value("PROCESSED")
    val FAILED = Value("FAILED")
    val REJECTED = Value("REJECTED")
    val OVERSIZED = Value("OVERSIZED")
  }

  object SegmentsState extends Enumeration {

    type SegState = Value
    val SKIPPED = Value("SKIPPED")
    val PROCESSED = Value("PROCESSED")
    val FAILED = Value("FAILED")
    val INVALID = Value("INVALID")
    val NOTAPPLICABLE = Value("N/A")
    val OVERSIZED = Value("OVERSIZED")
    val FILTERED = Value("FILTERED")

  }

  object OutFormats extends Enumeration {

    type OutFormat = Value
    val JSON = Value("JSON")
    val DELIMITED = Value("DELIMITED")

  }


  def rejectMsg(hl7: String, stage: String = EMPTYSTR, meta: MSGMeta, reason: String, data: mapType, t: Throwable = null, raw: String = EMPTYSTR): String = {
   s"$hl7$COLON$stage$PIPE_DELIMITED${meta.controlId}$PIPE_DELIMITED${meta.msgCreateTime}$PIPE_DELIMITED${meta.medical_record_num}"+
      s"$PIPE_DELIMITED${meta.medical_record_urn}$PIPE_DELIMITED${meta.account_num}$PIPE_DELIMITED$timeStamp$PIPE_DELIMITED"+
      (if (t != null) reason + (t.getStackTrace mkString repeat) else reason) + PIPE_DELIMITED + (if (raw ne EMPTYSTR) raw else toJson(data))
  }

  def rejectRawMsg(hl7: String, stage: String = EMPTYSTR, raw: String, reason: String, t: Throwable): String = {
    rejectMsg(hl7, stage, metaFromRaw(raw), reason, null, t, raw)
  }

  def metaFromRaw(raw: String): MSGMeta = {
    val delim = if (raw contains "\r\n") "\r\n" else "\n"
    val rawSplit = raw split delim
    valid(rawSplit, 1) match {
      case true => val msh = rawSplit(0)
        val temp = PIPER split msh
        Try(MSGMeta(temp(9), temp(6), EMPTYSTR, EMPTYSTR, EMPTYSTR, temp(3))) match {
          case Success(me) => me
          case _ => NONE
        }
      case _ => NONE
    }
  }


  def initSegStateWithZero: mutable.HashMap[SegState, Long] = {
    val temp = new mutable.HashMap[SegState, Long]()
    SegmentsState.values.foreach(x => temp += x -> 0L)
    temp
  }

  def segmentsForHl7Type(msgType: HL7, segments: List[(String, String)], delimitedBy: String = "\\^", modelFieldDelim: String = "|"): Hl7Segments = {
    import OutFormats._
    Hl7Segments(msgType, segments flatMap (seg => {
      seg._1 contains "ADHOC" match {
        case true =>
          val empty = Map.empty[String, String]
          val adhoc = seg._1 split COLON
          if (valid(adhoc, 3)) {
            val filterFile = Try(adhoc(4)) match {
              case Success(x) => x
              case _ => EMPTYSTR
            }
            val fieldWithNoAppends = Try(adhoc(5)) match {
              case Success(x) => x.split("\\&", -1)
              case _ => Array.empty[String]
            }
            val segStruc = adhoc take 2 mkString COLON
            val outFormats = adhoc(2) split "\\^"
            val outDest = adhoc(3) split "\\^"
            var index = -1
            outFormats.map(outFormat => {
              index += 1
              val outFormSplit = outFormat split AMPERSAND
              outFormat contains JSON.toString match {
                case true =>
                  (segStruc + COLON + outFormSplit(0), ADHOC(JSON, outDest(index), loadFile(outFormSplit(1), COMMA, keyIndex = 0), fieldWithNoAppends), loadFilters(filterFile))
                case _ =>
                  (segStruc + COLON + outFormSplit(0), ADHOC(DELIMITED, outDest(index), empty, fieldWithNoAppends), loadFilters(filterFile))
              }
            }).map(ad => Model(ad._1, seg._2, delimitedBy, modelFieldDelim, Some(ad._2), Some(ad._3))).toList
          } else throw new DataModelException("ADHOC Meta cannot be accepted. Please Check it " + seg._1)
        case _ =>
          seg._1 contains COLON match {
            case true =>
              val c = (seg._1 split COLON).map(_.split("\\" + AMPERSAND, -1))


              List(Model(seg._1, seg._2, delimitedBy, modelFieldDelim, None, None, Some(seg._1 split COLON)))
            case _ =>
              List(Model(seg._1, seg._2, delimitedBy, modelFieldDelim))
          }

      }
    }) groupBy (_.reqSeg))
  }
  case class SegmentsMapping(seg : String, mapping : Array[String])

  case class Hl7Segments(msgType: HL7, models: Map[String, List[Model]])

  case class HL7TransRec(rec: Either[(String, mutable.LinkedHashMap[String, Any], MSGMeta), Throwable])

  case class Hl7SegmentTrans(trans: Either[Traversable[(String, Throwable)], String])

  case class ADHOC(outFormat: OutFormat, dest: String, outKeyNames: Map[String, String], reqNoAppends: Array[String] = Array.empty[String])

  case class Model(reqSeg: String, segStr: String, delimitedBy: String = "\\"+repeat, modelFieldDelim: String = PIPE_DELIMITED,
                   adhoc: Option[ADHOC] = None, filters: Option[Array[FILTER]] = None, seqRelationMapping: Option[Array[String]] = None) extends modelLayout {
    lazy val modelFilter: Map[String, mutable.Set[String]] = synchronized(segFilter(segStr, delimitedBy, modelFieldDelim))
    lazy val EMPTY = mutable.LinkedHashMap.empty[String, String]

    override def getLayout: mutable.LinkedHashMap[String, String] = modelLayout(reqSeg, segStr, delimitedBy, modelFieldDelim, adhoc.isDefined)

    def layoutCopy: mutable.LinkedHashMap[String, String] = cachedLayout.clone.transform((k, v) => if ((k ne commonSegkey) & (v ne EMPTYSTR)) EMPTYSTR else v)

    def adhocLayout(layout: mutable.LinkedHashMap[String, String], keyNames: Map[String, String]): mutable.LinkedHashMap[String, String] = layout map { case (k, v) => keyNames(k) -> v }

  }

  private[model] sealed trait modelLayout {
    protected lazy val cachedLayout = getLayout

    def getLayout: mutable.LinkedHashMap[String, String]

  }

  case class MsgTypeMeta(msgType: com.hca.cdm.hl7.constants.HL7Types.HL7, kafka: String)

  def loadSegments(segments: String, delimitedBy: String = ","): Map[String, List[(String, String)]] = {
    val reader = Source.fromFile(segments).bufferedReader()
    val temp = Stream.continually(reader.readLine()).takeWhile(valid(_)).toList.map(seg => {
      val splits = seg split delimitedBy
      valid(splits, 3) match {
        case true => splits(0) -> (splits(1), splits(2))
        case _ => EMPTYSTR -> (EMPTYSTR, EMPTYSTR)
      }
    }).takeWhile(_._1 ne EMPTYSTR).groupBy(_._1).map({ case (k, v) => k -> v.map(x => x._2) })
    closeResource(reader)
    temp
  }

  def loadFilters(file: String, delimitedBy: String = ","): Array[FILTER] = {
    file match {
      case EMPTYSTR => Array.empty[FILTER]
      case _ =>
        Source.fromFile(file).getLines().takeWhile(valid(_)).map(temp => temp split delimitedBy) filter (valid(_, 5)) map {
          case x@ele => FILTER(x(0), (x(1), x(2)), (matchCriteria(x(3)), relationWithNextFilter(x(4))))
        } toArray
    }

  }

  def loadFile(file: String, delimitedBy: String = EQUAL, keyIndex: Int = 0): Map[String, String] = {
    Source.fromFile(file).getLines().takeWhile(valid(_)).map(temp => temp split delimitedBy) takeWhile (valid(_)) map {
      case x@ele if ele.nonEmpty => ele(keyIndex) -> x(keyIndex + 1)
    } toMap
  }

  def loadTemplate(template: String = "templateinfo.properties", delimitedBy: String = ","): Map[String, Map[String, Array[String]]] = {
    loadFile(template).map(file => {
      val reader = Source.fromFile(file._2).bufferedReader()
      val temp = Stream.continually(reader.readLine()).takeWhile(valid(_)).toList map (x => x split(delimitedBy, -1)) takeWhile (valid(_)) map (splits => {
        splits.head -> splits.tail
      })
      closeResource(reader)
      file._1 -> temp.toMap
    })
  }

  def getMsgTypeMeta(msgType: HL7, sourceIn: String): MsgTypeMeta = MsgTypeMeta(msgType, sourceIn)

  def handleAnyRef(data: AnyRef): String = {
    data match {
      case map: mapType => map.values mkString
      case list: listType => list map (map => map.values mkString) mkString
      case s: String => s
    }
  }

  private def segFilter(segmentData: String, delimitedBy: String, modelFieldDelim: String): Map[String, mutable.Set[String]] = synchronized {
    val temp = new mutable.HashMap[String, mutable.Set[String]] with mutable.MultiMap[String, String]
    (segmentData split(delimitedBy, -1)).filter(_ != EMPTYSTR) foreach (ele => {
      if (ele contains modelFieldDelim) {
        val repeats = ele split ("\\" + modelFieldDelim)
        repeats.length >= 2 match {
          case true => val k = repeats.head
            repeats.tail.foreach(v => temp addBinding(k, v))
          case _ => temp addBinding(ele, EMPTYSTR)
        }
      } else temp addBinding(ele, EMPTYSTR)
    })
    temp.toMap

  }


  private def modelLayout(whichSeg: String, segmentData: String, delimitedBy: String, modelFieldDelim: String,
                          isAdhoc: Boolean = false): mutable.LinkedHashMap[String, String] = synchronized {
    val layout = new mutable.LinkedHashMap[String, String]
    if (!isAdhoc) {
      layout += commonSegkey -> whichSeg
      commonNode.clone() transform ((k, v) => if (v ne EMPTYSTR) EMPTYSTR else v) foreach (ele => layout += ele)
    }
    (segmentData split(delimitedBy, -1)) foreach (ele => layout += ele -> EMPTYSTR)
    layout
  }

}
