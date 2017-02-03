package com.hca.cdm.spark

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{KafkaUtils => KConsumer}
import org.apache.spark.streaming.{Duration,StreamingContext}
import org.apache.spark.streaming.StreamingContext.{getOrCreate => create}
import org.apache.spark.{SparkConf, SparkContext}
import com.hca.cdm._
import org.apache.spark.deploy.SparkHadoopUtil.{get => hdpUtil}
import scala.language.postfixOps

/**
  * Created by Devaraj Jonnadula on 8/18/2016.
  */
object Hl7SparkUtil {

  /**
    * Creates Spark Configuration from Config File Provided
    */
  def getConf(app: String, parHint: String, kafkaConsumer: Boolean = true): SparkConf = {
    val conf = new SparkConf()
    val rate = lookUpProp("hl7.batch.rate").toInt
    conf
      .set("spark.streaming.backpressure.enabled", lookUpProp("hl7.rate.control"))
      .set("spark.streaming.backpressure.pid.minRate", rate.toString)
      .set("spark.streaming.backpressure.pid.derived", "0.1")
    if (kafkaConsumer) conf.set("spark.streaming.kafka.maxRatePerPartition", (rate + (rate / 8)).toString)
    conf
  }


  def createStreamingContext(conf: SparkConf, timeUnit : Duration): StreamingContext = new StreamingContext(conf, timeUnit)

  /**
    * Creates Spark Streaming Context
    */
  def streamingContext(checkpointPath: String, newCtxIfNotExist: () => StreamingContext): StreamingContext = {
    val ctx = create(checkpointPath, newCtxIfNotExist, hdpUtil.conf, createOnError = false)
    ctx checkpoint checkpointPath
    ctx
  }

  /**
    * Creates Spark Context
    */
  private def getSparkCtx(conf: SparkConf): SparkContext = new SparkContext(conf)


  /**
    * Creates Streams by polling Data from Kafka
    */
  def stream(sparkStrCtx: StreamingContext, kafkaConsumerProp: Map[String, String], topics: Set[String]): InputDStream[(String, String)] =
  KConsumer.createDirectStream[String, String, StringDecoder, StringDecoder](sparkStrCtx, kafkaConsumerProp, topics)


  /**
    * Shutdowns Streaming Context Gracefully allowing already Executing tasks to be Completed
    */
  def shutdownStreaming(sparkStrCtx: StreamingContext): Unit = if (sparkStrCtx != null) sparkStrCtx stop(stopSparkContext = false, stopGracefully = true)


  /**
    * Shutdowns Spark Context Gracefully allowing already Executing tasks to be Completed
    */
  def shutdownContext(sparkCtx: SparkContext): Unit = if (sparkCtx != null) sparkCtx stop

  /**
    * Shutdowns Spark context and Streaming Context Gracefully allowing already Executing tasks to be Completed
    */
  def shutdownEverything(sparkStrCtx: StreamingContext): Unit = if (sparkStrCtx != null) sparkStrCtx stop(stopSparkContext = true, stopGracefully = true)


}

