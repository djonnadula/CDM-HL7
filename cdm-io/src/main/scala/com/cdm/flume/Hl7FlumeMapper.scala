package com.cdm.flume

import java.text.SimpleDateFormat
import java.util
import java.util.Date
import com.cdm.log.Logg
import org.apache.flume.Event
import org.apache.flume.interceptor.Interceptor
import com.cdm._
import scala.language.postfixOps
import scala.util.{Success, Try}
import collection.JavaConverters._

/**
  * Created by Devaraj Jonnadula on 11/3/2016.
  */
class Hl7FlumeMapper extends Logg with Interceptor {

  private val KEY = "key"
  private val From_Topic = "topic"
  private val partitionMapping = Map(0 -> "landingZone", 1 -> "messageType", 2 -> "transactionDate")
  private val subPartMapping = Map(0 -> "hl7Segment")
  private lazy val unknownMapping = "unknown"
  private val outDateFormat = new SimpleDateFormat("yyyy-MM-dd")
  private val inDateFormat = new SimpleDateFormat("yyyyMMdd")
  private lazy val COLON = ":"

  override def close(): Unit = {}

  override def initialize(): Unit = {}

  override def intercept(event: Event): Event = {
    val header = event.getHeaders
    debug("Event Header :: " + header.toString)
    if (header.containsKey(KEY)) {
      header.getOrDefault(KEY, EMPTYSTR) match {
        case EMPTYSTR =>
          val topic = header getOrDefault (From_Topic,EMPTYSTR)
          if (topic == EMPTYSTR) {
            header put(partitionMapping(0), unknownMapping)
            warn(s"key Should Come As Part of Event, But Key came as Empty . Event header ${event.getHeaders} Cannot Route Message so Moving To Unknown Zone ::  $event")
          } else {
            header put(partitionMapping(0), topic)
            warn(s"key Should Come As Part of Event, But Key came as Empty . Event header ${event.getHeaders} Cannot Route Message so Moving To Zone as Topic $topic  ::  $event")
          }
          header put(partitionMapping(1), unknownMapping)
          header put(partitionMapping(2), outDateFormat.format(new Date()))
          event setHeaders header
        case key =>
          val split = key split(s"\\$COLON", -1)
          if (valid(split, partitionMapping.size)) {
            for (index <- split.indices) handlePartitionByIndex(index, split(index), header)(key)
            event setHeaders header
          } else {

          }
      }
    } else error(s"No Key In Event for Routing to appropriate location ${header.toString}")
    event
  }

  override def intercept(list: util.List[Event]): util.List[Event] = list.asScala.map(event => intercept(event)).asJava

  private def handlePartitionByIndex(index: Int, partition: String, mapper: util.Map[String, String])(key: String): Unit = {
    index match {
      case 0 =>
        mapper put(partitionMapping(1), partition)
      case 1 =>
        val split = partition split(s"\\$AMPERSAND", -1)
        if (valid(split)) split length match {
          case 1 =>
            mapper put(partitionMapping(0), split(0))
          case 2 =>
            mapper put(partitionMapping(0), split(0))
            mapper put(subPartMapping(0), split(1))
          case _ =>
        }
      case 2 =>
        mapper put(partitionMapping(2), parseDate(partition)(key))
      case _ =>
    }
  }

  private def parseDate(date: String)(key: String) = {
    Try(outDateFormat.format(inDateFormat.parse(date.substring(0, 8)))) match {
      case Success(x) => x
      case _ =>
        error(s"Create Date Time Cannot be Parsed :: $date Using System Time as Creation Time from Key $key")
        outDateFormat.format(new Date())
    }
  }

}

