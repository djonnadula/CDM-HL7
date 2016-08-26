package com.hca.cdm.spark.config

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{KafkaUtils => KConsumer}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by Devaraj Jonnadula on 8/18/2016.
  */
object Hl7SparkUtil {

  private lazy val conf = new SparkConf()

  def getConf(app: String, parHint: String): SparkConf = conf.setAppName(app).set("spark.default.parallelism", parHint)
    .set("spark.streaming.receiver.writeAheadLog.enable", "true")
    .set("spark.streaming.unpersist", "true")

  def getStreamingContext(batchCycle: String, conf : SparkConf): StreamingContext = {
    new StreamingContext(getSparkCtx, Seconds(batchCycle.toInt))
  }

  def getSparkCtx: SparkContext = {
    new SparkContext(conf)
  }

  def stream(sparkStrCtx: StreamingContext, kafkaConsumerProp: Map[String, String], topics: String): InputDStream[(String, String)] = {
    KConsumer.createDirectStream[String, String, StringDecoder, StringDecoder](sparkStrCtx, kafkaConsumerProp, Set(topics))
  }


}
