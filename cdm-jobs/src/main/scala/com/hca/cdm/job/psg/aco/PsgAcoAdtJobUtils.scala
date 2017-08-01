package com.hca.cdm.job.psg.aco

import java.io.{BufferedReader, InputStreamReader}

import com.hca.cdm._
import com.hca.cdm.hl7.constants.HL7Constants._
import com.hca.cdm.log.Logg
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

/**
  * Created by dof7475 on 7/20/2017.
  */
object PsgAcoAdtJobUtils extends Logg {

  def splitAndReturn(segment: String, delimiter: String, returnIndex: Int): Try[String] = {
    Try(segment.split(delimiter)(returnIndex))
  }

  def segment(message: Array[String], segType: String): Array[String] = {
    message.filter(segment => segment.startsWith(segType))
  }

  def readFile(path: Path, fileSystem: FileSystem): Array[AnyRef] = {
    val br = new BufferedReader(new InputStreamReader(fileSystem.open(path)))
    br.lines().toArray
  }

  def trySplit(message: String, delimiter: String): Try[Array[String]] = {
    Try(message.split(delimiter))
  }

  def eventTypeMatch(mshSegment: Array[String], adtTypes: Array[String]): Boolean = {
    mshSegment.headOption match {
      case Some(segment) =>
        splitAndReturn(segment, "\\|", 8) match {
          case Success(messageType) =>
            splitAndReturn(messageType, "\\^", 1) match {
              case Success(eventType) =>
                info(s"Found an event type: $eventType")
                adtTypes.contains(eventType)
              case Failure(t) =>
                warn("MSH Segment does not contain an event type")
                false
            }
          case Failure(t) =>
            error("MSH Segment does not contain a message type")
            false
        }
      case None =>
        error("Message does not contain MSH segment")
        false
    }
  }

  def singleFieldMatch[T](segment: Array[String], compareArray: Array[T], delimiter: String, index: Int): Boolean = {
    segment.headOption match {
      case Some(seg) =>
        splitAndReturn(seg, delimiter, index) match {
          case Success(targetIndex) =>
            info(s"Found index: $targetIndex")
            compareArray.contains(targetIndex)
          case Failure(t) =>
            warn("Segment does not contain target index")
            false
        }
      case None =>
        error("Message does not contain segment type")
        false
    }
  }

  def stringMatcher[T](segment: Array[String], compareArray: Array[T], delimiter: String, index: Int): Boolean = {
    segment.headOption match {
      case Some(seg) =>
        splitAndReturn(seg, delimiter, index) match {
          case Success(targetIndex) =>
            info(s"Found index: $targetIndex")
            compareArray.exists(t => targetIndex.toUpperCase().contains(t.toString.toUpperCase()))
          case Failure(t) =>
            warn("Segment does not contain matching string name")
            false
        }
      case None =>
        error("Message does not contain segment type")
        false
    }
  }

  def removeField(segment: Array[String], delimiter: String, index: Int): String = {
    segment.headOption match {
      case Some(seg) =>
        info(s"current seg: $seg")
        val splitSeg = seg.split(delimiter)
        if (splitSeg(index).nonEmpty) {
          splitSeg.update(index, "")
          splitSeg.mkString("|")
        } else {
          info(s"Segment contains no value at index: $index")
          seg
        }
      case None =>
        error("Message does not contain segment")
        ""
    }
  }

  def getField(segment: Array[String], delimiter: String, index: Int): String = {
    segment.headOption match {
      case Some(seg) =>
        info(s"current seg: $seg")
        splitAndReturn(seg, delimiter, index) match {
          case Success(targetIndex) =>
            info(s"Found index: $targetIndex")
            targetIndex
          case Failure(t) =>
            warn(s"Segment does not contain value at $index")
            ""
        }
      case None =>
        error("Message does not contain segment")
        ""
    }
  }

  @deprecated
  def findInsuranceIds(splitted: Array[String]): ArrayBuffer[String] = {
    val insuranceIds = new ArrayBuffer[String]
    splitted.foreach(segment => {
      if (segment.startsWith(IN1)) {
        splitAndReturn(segment, "\\|", 36) match {
          case Success(res) =>
            info(s"Found policy_num: $res")
            insuranceIds += res
          case Failure(t) => warn(s"No policy_num for segment")
        }
      }
    })
    insuranceIds
  }
}
