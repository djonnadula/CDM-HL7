package com.hca.cdm.spark

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{KafkaUtils => KConsumer}
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext.{getOrCreate => create}
import org.apache.spark.{SparkConf, SparkContext, SparkEnv => sparkEnv}
import com.hca.cdm._
import org.apache.spark.deploy.SparkHadoopUtil.{get => hdpUtil}
import java.lang.Class.{forName => className}
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}
import com.hca.cdm.kafka.util.TopicUtil._
import com.hca.cdm.log.Logg
import org.apache.spark.storage.StorageLevel

/**
  * Created by Devaraj Jonnadula on 8/18/2016.
  */
object Hl7SparkUtil extends Logg {

  private lazy val hookManager: Class[_] = {
    Try(className("org.apache.spark.util.ShutdownHookManager")) match {
      case Success(manager) =>
        manager
      case Failure(t) => null
    }
  }

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
    if (kafkaConsumer) {
      conf.set("spark.streaming.kafka.maxRatePerPartition", (rate + (rate / 8)).toString)
      conf.set("spark.streaming.kafka.maxRetries", tryAndReturnDefaultValue[String](asFunc(lookUpProp("hl7.spark.kafka.retries")), "15"))
    }
    conf
  }


  def createStreamingContext(conf: SparkConf, timeUnit: Duration): StreamingContext = new StreamingContext(conf, timeUnit)

  /**
    * Creates Spark Streaming Context
    */
  def streamingContext(checkpointPath: String, newCtxIfNotExist: () => StreamingContext): StreamingContext = {
    val ctx = create(checkpointPath, newCtxIfNotExist, hdpUtil.conf, createOnError = false)
    ctx checkpoint checkpointPath
    ctx.sparkContext.getConf.getAll.foreach(x => info(x._1 + " :: " + x._2))
    ctx
  }

  /**
    * Creates Spark Context
    */
  def getSparkCtx(conf: SparkConf): SparkContext = new SparkContext(conf)


  /**
    * Creates Streams by polling Data from Kafka
    */
  def stream(sparkStrCtx: StreamingContext, kafkaConsumerProp: Map[String, String], topics: Set[String]): InputDStream[(String, String)] =
    KConsumer.createDirectStream[String, String, StringDecoder, StringDecoder](sparkStrCtx, kafkaConsumerProp, topics)

  /**
    * Creates Streams by polling Data from Kafka Receiver
    */
  def stream(sparkStrCtx: StreamingContext, kafkaConsumerProp: Map[String, String], topics: Seq[String], dataStorage: StorageLevel = StorageLevel.MEMORY_ONLY): InputDStream[(String, String)] =
    KConsumer.createStream[String, String, StringDecoder, StringDecoder](sparkStrCtx, kafkaConsumerProp, topicPartitions(topics), dataStorage)


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


  /**
    *
    * Call this only After Context initialised
    */
  def currentSparkEnv(): sparkEnv = sparkEnv.get

  def batchCycle(timeUnit: String, cycle: Int): Duration = {
    timeUnit match {
      case "seconds" => Seconds(cycle)
      case "ms" => Milliseconds(cycle)
      case "minutes" => Minutes(cycle)
      case _ => Seconds(cycle)
    }
  }

  def addHook(fun: () => Unit, runPriority: Int = 100): Unit = {
    synchronized {
      if (hookManager != null) {
        val addShutdownHook = hookManager.getMethod("addShutdownHook", classOf[Int], classOf[() => Unit])
        addShutdownHook.invoke(hookManager, Int.box(runPriority), hookRunner(fun))
      }
      else registerHook(newThread(s"$fun", runnable(fun())))
    }
  }

  private def hookRunner(hook: () => Unit): Object = {
    new (() => Object) {
      override def apply(): Object = {
        hook()
        EMPTYSTR
      }
    }
  }
}

