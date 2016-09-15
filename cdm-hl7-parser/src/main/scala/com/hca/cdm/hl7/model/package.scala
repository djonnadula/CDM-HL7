package com.hca.cdm.hl7

import com.hca.cdm._
import com.hca.cdm.exception.CdmException
import com.hca.cdm.hl7.audit.MSGMeta
import com.hca.cdm.hl7.constants.HL7Constants._
import com.hca.cdm.hl7.constants.HL7Types._
import com.hca.cdm.hl7.model.OutFormats.OutFormat
import com.hca.cdm.hl7.model.SegmentsState.SegState
import com.hca.cdm.utils.DateUtil.{currentTimeStamp => timeStamp}

import scala.collection.mutable
import scala.io.Source
import scala.language.postfixOps
import scala.util.{Success, Try}

/**
  * Created by Devaraj Jonnadula on 8/22/2016.
  */
package object model {

  lazy val MSH_Segment = "0001.MSH"
  lazy val Message_Type_Segment = "009.msh_msg_type"
  lazy val Message_Control_Id = "001.message_code"
  lazy val Msg_Type_Hier = Seq(MSH_Segment, Message_Type_Segment, Message_Control_Id)
  lazy val Observation_Col = "005.obx_observation_value"
  lazy val MSH_INDEX = "0001.MSH"
  lazy val PID_INDEX = "0003.PID"
  lazy val commonNodeStr = "0000.COMN"
  lazy val OBX_SEG = "OBX"
  lazy val commonmSegkey = "seg"
  lazy val notValidStr = "Not Valid Input"
  lazy val skippedStr = "SKIPPED"
  lazy val NA = "Not Applicable"
  private lazy val NONE = MSGMeta("no message control ID", "no time from message", EMPTYSTR, EMPTYSTR, EMPTYSTR)
  lazy val repeat = "^"
  val commonNode = synchronized {
    val temp = new mutable.LinkedHashMap[String, String]
    val nodeEle = loopUpProp("common.elements") match {
      case EMPTYSTR => throw new CdmException("No Common Elements found. ")
      case valid => valid split("\\^", -1)
    }
    nodeEle.foreach(ele => temp += ele -> EMPTYSTR)
    temp
  }


  object SegmentsState extends Enumeration {

    type SegState = Value
    val SKIPPED = Value("SKIPPED")
    val PROCESSED = Value("PROCESSED")
    val FAILED = Value("FAILED")
    val INVALID = Value("INVALID")
    val NOTAPPLICABLE = Value("N/A")

  }

  object OutFormats extends Enumeration {

    type OutFormat = Value
    val JSON = Value("JSON")
    val DELIMITED = Value("DELIMITED")

  }

  def rejectMsg(hl7: String, stage: String = EMPTYSTR, meta: MSGMeta, reason: String, data: mapType, t: Throwable = null, raw :String = EMPTYSTR): String = {
    hl7 + COLON + stage + PIPE_DELIMITED + meta.controlId + PIPE_DELIMITED + meta.msgCreateTime + PIPE_DELIMITED + meta.medical_record_num +
      PIPE_DELIMITED + meta.medical_record_urn + PIPE_DELIMITED + meta.account_num + PIPE_DELIMITED + timeStamp + PIPE_DELIMITED +
      (if (t != null) reason + (t.getStackTrace mkString repeat) else reason) + PIPE_DELIMITED +  (if(raw ne EMPTYSTR) raw else serialize(data))

  }

  def rejectRawMsg(hl7: String, stage: String = EMPTYSTR, raw: String, reason: String, t: Throwable): String = {
    var meta = NONE
    val delim = if (raw contains "\r\n") "\r\n" else "\n"
    val rawSplit = raw split delim
    valid(rawSplit, 1) match {
      case true => val msh = rawSplit(0)
        val temp = PIPER split msh
        meta = Try(MSGMeta(temp(0), temp(6), EMPTYSTR, EMPTYSTR, EMPTYSTR)) match {
          case Success(me) => me
          case _ => NONE
        }
      case _ => NONE
    }
    rejectMsg(hl7, stage, meta, reason, null, t, raw)
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
            val segStruc = adhoc take 2 mkString COLON
            val outFormats = adhoc(2) split "\\^"
            val outDest = adhoc(3) split "\\^"
            var index = -1
            outFormats.map(outFormat => {
              index += 1
              val outFormSplit = outFormat split "&"
              outFormat contains JSON.toString match {
                case true => (segStruc + COLON + outFormSplit(0), ADHOC(JSON, outDest(index), loadFile(loopUpProp("hl7.app.home"), outFormSplit(1), COMMA, keyIndex = 0)))
                case _ => (segStruc + COLON + outFormSplit(0), ADHOC(DELIMITED, outDest(index), empty))
              }
            }).map(ad => Model(ad._1, seg._2, delimitedBy, modelFieldDelim, Some(ad._2))).toList
          } else throw new DataModelException("ADHOC Meta cannot be accepted. Please Check it " + seg._1)
        case _ => List(Model(seg._1, seg._2, delimitedBy, modelFieldDelim))
      }
    }) groupBy (_.reqSeg))
  }

  case class Hl7Segments(msgType: HL7, models: Map[String, List[Model]])

  case class HL7TransRec(rec: Either[(String, mutable.LinkedHashMap[String, Any]), Throwable])

  case class Hl7SegmentTrans(trans: Either[Traversable[(String, Throwable)], String])

  case class ADHOC(outFormat: OutFormat, dest: String, outKeyNames: Map[String, String])

  case class Model(reqSeg: String, segStr: String, delimitedBy: String = "\\^", modelFieldDelim: String = "|", adhoc: Option[ADHOC] = None) extends modelLayout {
    lazy val modelFilter: Map[String, mutable.Set[String]] = segFilter(segStr, delimitedBy, modelFieldDelim)
    lazy val EMPTY = mutable.LinkedHashMap.empty[String, String]

    override def getLayout: mutable.LinkedHashMap[String, String] = modelLayout(reqSeg, segStr, delimitedBy, modelFieldDelim, adhoc.isDefined)

    def layoutCopy: mutable.LinkedHashMap[String, String] = cachedLayout.clone.transform((k, v) => if ((k ne commonmSegkey) & (v ne EMPTYSTR)) EMPTYSTR else v)

    def adhocLayout(layout: mutable.LinkedHashMap[String, String], keyNames: Map[String, String]): mutable.LinkedHashMap[String, String] = layout map { case (k, v) => keyNames(k) -> v }


  }


  private[model] sealed trait modelLayout {
    protected lazy val cachedLayout = getLayout

    def getLayout: mutable.LinkedHashMap[String, String]


  }

  case class MsgTypeMeta(msgType: com.hca.cdm.hl7.constants.HL7Types.HL7, appHome: String, template: String = "templateinfo.properties", segments: String = "segments.csv", kafka: String)

  def loadSegments(meta: MsgTypeMeta, delimitedBy: String = ","): List[(String, String)] = {
    val reader = Source.fromFile(meta.appHome + FS + meta.msgType.toString + FS + meta.segments).bufferedReader()
    val temp = Stream.continually(reader.readLine()).takeWhile(valid(_)).toList.map(seg => {
      val splits = seg split delimitedBy
      valid(splits, 3) match {
        case true => (splits(1), splits(2))
        case _ => (EMPTYSTR, EMPTYSTR)
      }
    }).takeWhile(_._1 ne EMPTYSTR)
    closeResource(reader)
    temp
  }


  def loadFile(appHome: String, file: String, delimitedBy: String = "=", keyIndex: Int = 0): Map[String, String] = {
    Source.fromFile(appHome + FS + file).getLines().takeWhile(valid(_)).map(temp => temp split delimitedBy) takeWhile (valid(_)) map {
      case x@ele if ele.nonEmpty => ele(keyIndex) -> x(keyIndex + 1)
    } toMap
  }

  def loadTemplate(meta: MsgTypeMeta, delimitedBy: String = ","): Map[String, Map[String, Array[String]]] = {
    loadFile(meta.appHome + FS + meta.msgType.toString, meta.template).map(file => {
      val reader = Source.fromFile(meta.appHome + FS + meta.msgType.toString + FS + file._2).bufferedReader()
      val temp = Stream.continually(reader.readLine()).takeWhile(valid(_)).toList map (x => x split(delimitedBy, -1)) takeWhile (valid(_)) map (splits => {
        splits.head -> splits.tail
      })
      closeResource(reader)
      file._1 -> temp.toMap
    })

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
      layout += commonmSegkey -> whichSeg
      commonNode.clone() transform ((k, v) => if (v ne EMPTYSTR) EMPTYSTR else v) foreach (ele => layout += ele)
    }
    (segmentData split(delimitedBy, -1)) foreach (ele => {
      if (ele contains modelFieldDelim) {
        val repeats = ele split ("\\" + modelFieldDelim)
        val key = repeats.head
        repeats.tail.foreach(x => layout += key + modelFieldDelim + x -> EMPTYSTR)
      }
      else layout += ele -> EMPTYSTR
    })
    layout
  }

}
