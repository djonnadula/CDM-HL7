package com.hca.cdm.flume

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

/**
  * Created by Devaraj Jonnadula on 9/8/2017.
  */
private[cdm] class DigitalReasonTransformBuilder extends Builder {
  private var dataOutMapping = new mutable.LinkedHashMap[String, String]
  private var mandateFields: Set[String] = _
  private var delimiter = "|"


  override def build(): Interceptor = new DigitalReasonDataTransformer(delimiter, dataOutMapping, mandateFields)

  override def configure(context: Context): Unit = {
    context.getString("outboundSchema", EMPTYSTR).split("\\^").foreach(x => dataOutMapping += x -> EMPTYSTR)
    mandateFields = context.getString("fieldsToValidate", EMPTYSTR).split("\\^").toSet
    delimiter = context.getString("delimiter", "|")
  }
}

/**
  * Created by Devaraj Jonnadula on 9/8/2017.
  */
private[cdm] class DigitalReasonDataTransformer(delimiter: String, outboundSchema: mutable.LinkedHashMap[String, String], mandateFields: Set[String]) extends Logg with Interceptor {
  private val mapper = new ObjectMapper().readValue(_: Array[Byte], classOf[util.HashMap[String, String]])
  private val Malformed = new util.HashMap[String, String]()

  override def close(): Unit = {

  }

  override def intercept(event: Event): Event = {
    val data = tryAndReturnDefaultValue0(mapper(event.getBody), Malformed)
    if (validate(data)) {
      tryAndLogErrorMes(event.setBody(outboundSchema.clone().transform { case (k, _) => data.getOrDefault(k, EMPTYSTR) }.values.mkString(delimiter).getBytes), error(_: Throwable))
      updateHeader(event.getHeaders)
    }
    else updateHeader(event.getHeaders, valid = false)
    event
  }

  override def intercept(events: util.List[Event]): util.List[Event] = events.asScala.map(intercept).filter(valid(_)).asJava

  override def initialize(): Unit = {
  }

  private def updateHeader(header: util.Map[String, String], valid: Boolean = true): Unit = {
    if (valid) header put("landingZone", "valid")
    header put("landingZone", "rejected")
  }

  private def validate(data: util.HashMap[String, String]): Boolean = {
    if (data == Malformed) return false
    mandateFields foreach { field =>
      if ((data.getOrDefault(field, EMPTYSTR) == EMPTYSTR) || (data.get(field) == null)) return false
    }
    true
  }
}
