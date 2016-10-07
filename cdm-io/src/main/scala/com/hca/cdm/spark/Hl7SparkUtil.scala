package com.hca.cdm.spark

import kafka.serializer.StringDecoder
import org.apache.spark.launcher.SparkLauncher._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{KafkaUtils => KConsumer}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import com.hca.cdm._

/**
  * Created by Devaraj Jonnadula on 8/18/2016.
  */
object Hl7SparkUtil {

  def getConf(app: String, parHint: String): SparkConf = new SparkConf()
    .setAppName(app)
    .set("spark.default.parallelism", parHint)
    .setMaster(lookUpProp("hl7.spark.master"))
    .set(EXECUTOR_MEMORY, lookUpProp("hl7.spark.executor-memory"))
    .set(EXECUTOR_CORES, parHint)
    .set("spark.driver-memory", lookUpProp("hl7.spark.driver-memory"))
    .set("spark.dynamicAllocation.initialExecutors", lookUpProp("hl7.spark.num-executors"))
    .set("spark.queue", lookUpProp("hl7.spark.queue"))
    .set("spark.dynamicAllocation.enabled", lookUpProp("hl7.spark.dynamicAllocation.enabled"))
    .set("spark.dynamicAllocation.maxExecutors", lookUpProp("hl7.spark.dynamicAllocation.maxExecutors"))
    .set("spark.dynamicAllocation.minExecutors", lookUpProp("hl7.spark.dynamicAllocation.minExecutors"))
    .set("spark.driver.maxResultSize", lookUpProp("hl7.spark.driver.maxResultSize"))
    .set("spark.streaming.receiver.writeAheadLog.enable", "true")
    .set("spark.streaming.unpersist", "true")
    .set("spark.streaming.kafka.maxRatePerPartition", "200")

  def getStreamingContext(batchCycle: Int, conf: SparkConf): StreamingContext = new StreamingContext(conf, Seconds(batchCycle))


  def getSparkCtx(conf: SparkConf): SparkContext = new SparkContext(conf)


  def stream(sparkStrCtx: StreamingContext, kafkaConsumerProp: Map[String, String], topics: Set[String]): InputDStream[(String, String)] =
    KConsumer.createDirectStream[String, String, StringDecoder, StringDecoder](sparkStrCtx, kafkaConsumerProp, topics)


  def shutdown(sparkStrCtx: StreamingContext): Unit = if (sparkStrCtx != null) sparkStrCtx stop(stopSparkContext = true, stopGracefully = true)


}
