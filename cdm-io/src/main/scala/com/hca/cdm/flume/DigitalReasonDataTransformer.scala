package com.hca.cdm.flume

import java.text.SimpleDateFormat
import java.util
import com.fasterxml.jackson.databind.ObjectMapper
import com.hca.cdm.log.Logg
import com.hca.cdm._
import scala.language.postfixOps
import collection.JavaConverters._
import org.apache.flume.{Context, Event}
import org.apache.flume.interceptor.Interceptor
import org.apache.flume.interceptor.Interceptor.Builder
import scala.collection.mutable
import com.hca.cdm.utils.{DateConstants, DateUtil}

/**
  * Created by Devaraj Jonnadula on 9/8/2017.
  */
private[cdm] class DigitalReasonTransformBuilder extends Builder {
  private var dataOutMapping = new mutable.LinkedHashMap[String, String]
  private var mandateFields: Set[String] = _
  private var stripOutFields: Map[String, String] = Map()
  private var fieldsMerge: List[(Array[String], String, String)] = List.empty
  private var delimiter = "|"


  override def build(): Interceptor = new DigitalReasonDataTransformer(delimiter, dataOutMapping, mandateFields, stripOutFields, fieldsMerge)

  override def configure(context: Context): Unit = {
    context.getString("outboundSchema", EMPTYSTR).split("\\^").foreach(x => dataOutMapping += x -> EMPTYSTR)
    mandateFields = context.getString("fieldsToValidate", EMPTYSTR).split("\\^").toSet
    stripOutFields = context.getString("stripOutFields", EMPTYSTR).split("\\^").map(field => field -> EMPTYSTR).toMap
    fieldsMerge = context.getString("fieldsMerge", EMPTYSTR).split("\\,", -1).toList.map {
      x =>
        val split = x.split("\\:", -1)
        val temp = split.splitAt(split.length - 2)
        (temp._1(0).split(AMPERSAND), temp._2(0), temp._2(1))
    }
    delimiter = context.getString("delimiter", "|")
  }
}

/**
  * Created by Devaraj Jonnadula on 9/8/2017.
  */
private[cdm] class DigitalReasonDataTransformer(delimiter: String, outboundSchema: mutable.LinkedHashMap[String, String],
                                                mandateFields: Set[String], stripOutFields: Map[String, String], fieldsMerge: List[(Array[String], String, String)]) extends Logg with Interceptor {
  private val mapper = new ObjectMapper().readValue(_: Array[Byte], classOf[util.HashMap[String, String]])
  private val Malformed = new util.HashMap[String, String]()
  private val outDateFormat = new SimpleDateFormat(DateConstants.PATID_FORMAT)
  private val inDateFormat = new SimpleDateFormat(DateConstants.DATE_PATTERN_YYYYMMDD)

  override def close(): Unit = {

  }

  override def intercept(event: Event): Event = {
    val data = tryAndReturnDefaultValue0(mapper(event.getBody), Malformed)
    if (validate(data)) {
      fieldsMerge.foreach {
        case (criteria, modify, delimitedBy) =>
          if (modify == "user_action_date_time") {
            val actionDt = criteria.map(field => data.getOrDefault(field, EMPTYSTR)).mkString(delimitedBy)
            if (valid(actionDt) && actionDt.length == 8) {
              data put(modify, tryAndReturnDefaultValue0(outDateFormat.format(inDateFormat.parse(actionDt)), actionDt))
            } else data put(modify, actionDt)
          } else {
            data put(modify, criteria.map(field => data.getOrDefault(field, EMPTYSTR)).mkString(delimitedBy))
          }
      }
      tryAndLogErrorMes(event.setBody(outboundSchema.clone().transform {
        case (k, _) =>
          if (stripOutFields isDefinedAt k) {
            val temp = data.getOrDefault(k, EMPTYSTR)
            if (temp contains "^") tryAndReturnDefaultValue0(temp.split("\\^", -1).head, EMPTYSTR)
            else temp
          }
          else data.getOrDefault(k, EMPTYSTR)
      }.values.mkString(delimiter).getBytes), error(_: Throwable))
      updateHeader(event.getHeaders)
    }
    else updateHeader(event.getHeaders, valid = false)
    event
  }

  override def intercept(events: util.List[Event]): util.List[Event] = events.asScala.map(intercept).asJava

  override def initialize(): Unit = {
  }

  private def updateHeader(header: util.Map[String, String], valid: Boolean = true): Unit = {
    if (valid) header put("landingZone", "valid")
    else header put("landingZone", "rejected")
  }

  private def validate(data: util.HashMap[String, String]): Boolean = {
    if (data == Malformed) return false
    mandateFields foreach { field =>
      if ((data.getOrDefault(field, EMPTYSTR) == EMPTYSTR) || (data.get(field) == null)) return false
    }
    true
  }
}
