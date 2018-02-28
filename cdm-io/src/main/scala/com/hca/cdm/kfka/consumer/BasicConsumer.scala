/*
package com.hca.cdm.kfka.consumer


import kafka.consumer._
import kafka.serializer.DefaultDecoder
import org.apache.kafka.streams.kstream.KStream
import java.lang.Long
import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.{KeyValueMapper,  ValueMapper}
import org.apache.kafka.streams.state.KeyValueStore;

import scala.collection.JavaConverters.asJavaIterableConverter
/**
  * Created by Devaraj Jonnadula on 2/2/2018.
  */
class BasicConsumer(topic: String,  yourId: String) extends AutoCloseable  {
  val valid = validate(topic,yourId)
  require(valid._1,valid._2)
  val builder: StreamsBuilder = new StreamsBuilder()
  val textLines: KStream[String, String] = builder.stream("TextLinesTopic")
  private val consumerConnector = Consumer.create(new ConsumerConfig(FCMConsumerConfig.createBasicConfig(yourId)))

  private val itr = consumerConnector.createMessageStreamsByFilter(new Whitelist(topic), 1, new DefaultDecoder(), new DefaultDecoder()).head.iterator

  override def close: Unit = consumerConnector.shutdown

  def hasNext: Boolean = if (itr != null && itr.hasNext) true else  false

  def next: Array[ Byte ] = if (itr != null) itr.next.message  else null

  private def validate(topic:String, ID: String) : (Boolean,String)= {
    val validReq = topic != null && ID != null && !topic.trim.isEmpty && !ID.trim.isEmpty
    else (validReq, "InValid Initialisation")
  }

}
*/
