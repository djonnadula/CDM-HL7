package com.hca.cdm.flume

import java.util
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.hca.cdm.log.Logg
import com.hca.cdm._
import scala.language.postfixOps
import collection.JavaConverters._
import org.apache.flume.{Context, Event}
import org.apache.flume.interceptor.Interceptor
import org.apache.flume.interceptor.Interceptor.Builder
import scala.collection.mutable


private[cdm] class DigitalReasonTransformBuilder extends Builder {
  private var dataOutMapping = new mutable.LinkedHashMap[String, String]
  private var delimiter = "|"


  override def build(): Interceptor = new DigitalReasonDataTransformer(delimiter, dataOutMapping)

  override def configure(context: Context): Unit = {
    context.getString("outboundSchema", EMPTYSTR).split("\\^").foreach(x => dataOutMapping += x -> EMPTYSTR)
    delimiter = context.getString("delimiter", "|")
  }
}

/**
  * Created by Devaraj Jonnadula on 9/8/2017.
  */
private[cdm] class DigitalReasonDataTransformer(delimiter: String, outboundSchema: mutable.LinkedHashMap[String, String]) extends Logg with Interceptor {
  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule).readValue(_: Array[Byte], classOf[mutable.LinkedHashMap[String, String]])

  override def close(): Unit = {

  }

  override def intercept(event: Event): Event = {
    val data = mapper(event.getBody)
    tryAndLogErrorMes(event.setBody(outboundSchema.clone().transform { case (k, v) => data.getOrElse(k, EMPTYSTR) }.mkString(delimiter).getBytes), error(_: Throwable))
    event
  }

  override def intercept(events: util.List[Event]): util.List[Event] = events.asScala.map(intercept).asJava

  override def initialize(): Unit = {

  }
}
