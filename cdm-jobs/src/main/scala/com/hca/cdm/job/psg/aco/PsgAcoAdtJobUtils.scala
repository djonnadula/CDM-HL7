package com.hca.cdm.job.psg.aco

import java.io.{BufferedReader, InputStreamReader}

import com.hca.cdm.log.Logg
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.util.{Failure, Success, Try}

/**
  * PSG ACO ADT Util methods
  */
object PsgAcoAdtJobUtils extends Logg {

  /**
    * Tries to split a string on the given delimiter and return the specified index
    * @param segment string split candidate
    * @param delimiter delimiter
    * @param returnIndex index to return
    * @return [[Try]] split index
    */
  def splitAndReturn(segment: String, delimiter: String, returnIndex: Int): Try[String] = {
    Try(segment.split(delimiter)(returnIndex))
  }

  /**
    * Filter an array to only those indexes that start with a given string
    * @param message string array to filter
    * @param segType string to search by startsWith
    * @return an array of segments that start with segType
    */
  def segment(message: Array[String], segType: String): Array[String] = {
    message.filter(segment => segment.startsWith(segType))
  }

  /**
    * Read a file and return an array of lines
    * @param path file path
    * @param fileSystem filesystem object
    * @return array of lines from the file
    */
  def readFileAsArray(path: Path, fileSystem: FileSystem): Array[AnyRef] = {
    val br = new BufferedReader(new InputStreamReader(fileSystem.open(path)))
    br.lines().toArray
  }

  /**
    * Try to split a given string by a delimiter
    * @param message string to split
    * @param delimiter delimiter
    * @return [[Try]] split message
    */
  def trySplit(message: String, delimiter: String): Try[Array[String]] = {
    Try(message.split(delimiter))
  }

  /**
    * Returns true if the ADT event type exists in the adtTypes array; otherwise false
    * @param mshSegment MSH pipe delimited segment
    * @param adtTypes Array of ADT types to match
    * @return true if MSH contains a matching ADT type; otherwise false
    */
  def eventTypeMatch(mshSegment: Array[String], adtTypes: Array[String]): Boolean = {
    mshSegment.headOption match {
      case Some(segment) =>
        splitAndReturn(segment, "\\|", 8) match {
          case Success(messageType) =>
            splitAndReturn(messageType, "\\^", 1) match {
              case Success(eventType) =>
                info(s"Found an event type: $eventType")
                val indexExists = adtTypes.contains(eventType)
                debug(s"indexExists: $indexExists")
                indexExists
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

  /**
    * Checks if array contains a target index
    * @param segment hl7 segment array
    * @param compareArray array to compare to
    * @param delimiter delimiter to split by
    * @param index index to match
    * @tparam T any type
    * @return true if match; otherwise false
    */
  def singleFieldMatch[T](segment: Array[String], compareArray: Array[T], delimiter: String, index: Int): Boolean = {
    segment.headOption match {
      case Some(seg) =>
        splitAndReturn(seg, delimiter, index) match {
          case Success(targetIndex) =>
            info(s"Found index: $targetIndex")
            val indexExists = compareArray.contains(targetIndex)
            debug(s"indexExists: $indexExists")
            indexExists
          case Failure(t) =>
            warn("Segment does not contain target index")
            false
        }
      case None =>
        warn("Message does not contain segment type")
        false
    }
  }

  /**
    * checks if a given string exists in an array
    * @param segment segment to get index from
    * @param compareArray array to check
    * @param delimiter delimiter to split by
    * @param index target index to search
    * @tparam T any type
    * @return true if match; otherwise false
    */
  def stringMatcher[T](segment: Array[String], compareArray: Array[T], delimiter: String, index: Int): Boolean = {
    segment.headOption match {
      case Some(seg) =>
        splitAndReturn(seg, delimiter, index) match {
          case Success(targetIndex) =>
            info(s"Found index: $targetIndex")
            val indexExists = compareArray.exists(t => targetIndex.toUpperCase().contains(t.toString.toUpperCase()))
            debug(s"indexExists: $indexExists")
            indexExists
          case Failure(t) =>
            warn("Segment does not contain matching string name")
            false
        }
      case None =>
        warn("Message does not contain segment type")
        false
    }
  }

  /**
    * Update the index with an empty string
    * @param segment segment to remove index from
    * @param delimiter delimiter to split by
    * @param index index to remove
    * @return segment with index removed
    */
  def removeField(segment: Array[String], delimiter: String, index: Int): String = {
    segment.headOption match {
      case Some(seg) =>
        val splitSeg = seg.split(delimiter)
        if (splitSeg(index).nonEmpty) {
          splitSeg.update(index, "")
          splitSeg.mkString(delimiter)
        } else {
          info(s"Segment contains no value at index: $index")
          seg
        }
      case None =>
        warn("Message does not contain segment")
        ""
    }
  }

  /**
    * get a field at a specified index
    * @param segment segment to get field from
    * @param delimiter delimiter to split by
    * @param index target index
    * @return target string; otherwise empty string
    */
  def getField(segment: Array[String], delimiter: String, index: Int): String = {
    segment.headOption match {
      case Some(seg) =>
        debug(s"current seg: $seg")
        splitAndReturn(seg, delimiter, index) match {
          case Success(targetIndex) =>
            info(s"Found targetIndex")
            targetIndex
          case Failure(t) =>
            warn(s"Segment does not contain value at $index")
            ""
        }
      case None =>
        warn("Message does not contain segment")
        ""
    }
  }

}
