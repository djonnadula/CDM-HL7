package com.hca.cdm.hl7

import com.hca.cdm._
import com.hca.cdm.exception.CdmException
import com.hca.cdm.hl7.constants.HL7Types._
import com.hca.cdm.hl7.model.SegmentsState.SegState

import scala.collection.mutable
import scala.io.Source
import scala.language.postfixOps

/**
  * Created by Devaraj Jonnadula on 8/22/2016.
  */
package object model {
  lazy val segmentkey = "seg"
  lazy val notValidStr = "Not Valid Input"
  lazy val skippedStr = "SKIPPED"
  lazy val commonNodeStr = "0000.COMN"
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

  }

  def initSegStateWithZero: mutable.HashMap[SegState, Long] = {
    val temp = new mutable.HashMap[SegState, Long]()
    SegmentsState.values.foreach(x => temp += x -> 0L)
    temp
  }


  def segmentsForHl7Type(msgType: HL7, segments: List[(String, String)], delimitedBy: String = "\\^", modelFieldDelim: String = "|"): Hl7Segments = {
    Hl7Segments(msgType, segments.map(seg => {
      Model(seg._1, seg._2, delimitedBy, modelFieldDelim)
    }).groupBy(_.reqSeg))
  }

  case class Hl7Segments(msgType: HL7, models: Map[String, List[Model]])

  case class HL7TransRec(rec: Either[(String, mutable.LinkedHashMap[String, Any]), Throwable])

  case class Hl7SegmentTrans(seg: Either[Traversable[(String, Throwable)], String])

  case class Model(reqSeg: String, segStr: String, delimitedBy: String = "\\^", modelFieldDelim: String = "|") extends modelLayout {
    lazy val modelFilter: Map[String, mutable.Set[String]] = segFilter(segStr, delimitedBy, modelFieldDelim)
    lazy val EMPTY = mutable.LinkedHashMap.empty[String, String]

    override def getLayout: mutable.LinkedHashMap[String, String] = modelLayout(reqSeg, segStr, delimitedBy, modelFieldDelim)

    def layoutCopy: mutable.LinkedHashMap[String, String] = cachedLayout.clone.transform((k, v) => if ((k ne segmentkey) & (v ne EMPTYSTR)) EMPTYSTR else v)


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


  private def loadFile(appHome: String, file: String, delimitedBy: String = "="): Map[String, String] = {
    Source.fromFile(appHome + FS + file).getLines().takeWhile(valid(_)).map(temp => temp split delimitedBy) takeWhile (valid(_)) map {
      case x@ele if ele.nonEmpty => ele(0) -> x(1)
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


  private def modelLayout(whichSeg: String, segmentData: String, delimitedBy: String, modelFieldDelim: String): mutable.LinkedHashMap[String, String] = synchronized {
    val layout = new mutable.LinkedHashMap[String, String]
    layout += segmentkey -> whichSeg
    commonNode.clone().transform((k, v) => if (v ne EMPTYSTR) EMPTYSTR else v) foreach (ele => layout += ele)
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
