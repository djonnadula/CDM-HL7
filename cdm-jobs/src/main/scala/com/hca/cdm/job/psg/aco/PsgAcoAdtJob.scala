package com.hca.cdm.job.psg.aco

import java.lang.System.{getenv => fromEnv}
import java.util.Date

import com.hca.cdm._
import com.hca.cdm.kafka.config.HL7ConsumerConfig.{createConfig => consumerConf}
import com.hca.cdm.log.Logg
import com.hca.cdm.spark.{Hl7SparkUtil => sparkUtil}
import com.hca.cdm.hl7.constants.HL7Constants._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.apache.spark.streaming.StreamingContext

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

/**
  * Created by dof7475 on 6/1/2017.
  */
object PsgAcoAdtJob extends Logg with App {

  self =>
  private val config_file = args(0)
  info("config_file: " + config_file)
  propFile = config_file
  private val app = lookUpProp("hl7.app")
  private val defaultPar = lookUpProp("hl7.spark.default.parallelism")
  private val sparkConf = sparkUtil.getConf(lookUpProp("hl7.app"), defaultPar)
  private val batchCycle = lookUpProp("hl7.batch.interval").toInt
  private val maxPartitions = defaultPar.toInt * lookUpProp("hl7.spark.dynamicAllocation.maxExecutors").toInt
  private val batchDuration = sparkUtil batchCycle(lookUpProp("hl7.batch.time.unit"), batchCycle)
  private val consumerGroup = lookUpProp("PSGACOADT.group")
  private val checkpointEnable = lookUpProp("PSGACOADT.spark.checkpoint.enable").toBoolean
  private val checkPoint = lookUpProp("PSGACOADT.checkpoint")
  private val topicsToSubscribe = Set(lookUpProp("PSGACOADT.kafka.source"))
  private val kafkaConsumerProp = (consumerConf(consumerGroup) asScala) toMap
  private val fileSystem = FileSystem.get(new Configuration())
  private val appHomeDir = fileSystem.getHomeDirectory.toString
  private val stagingDir = fromEnv("SPARK_YARN_STAGING_DIR")

  private var sparkStrCtx: StreamingContext = initContext
  printConfig()
  startStreams()

  private def newCtxIfNotExist = new (() => StreamingContext) {
    override def apply(): StreamingContext = {
      val ctx = sparkUtil createStreamingContext(sparkConf, batchDuration)
      info(s"New Checkpoint Created for $app $ctx")
      runJob(ctx)
      ctx
    }
  }

  private def initContext: StreamingContext = {
    sparkStrCtx = if (checkpointEnable) sparkUtil streamingContext(checkPoint, newCtxIfNotExist) else sparkUtil createStreamingContext(sparkConf, batchDuration)
    sparkStrCtx.sparkContext setJobDescription lookUpProp("job.desc")
    if (!checkpointEnable) runJob(sparkStrCtx)
    sparkStrCtx
  }

  def runJob(sparkStrCtx: StreamingContext): Unit = {
    val streamLine = sparkUtil stream(sparkStrCtx, kafkaConsumerProp, topicsToSubscribe)
    info("kafkaConsumerProp: " + kafkaConsumerProp)
    info("subscribed topics: " + topicsToSubscribe.mkString(","))
    info(s"Kafka Stream Was Opened Successfully with ID :: ${streamLine.id}")
    streamLine foreachRDD (rdd => {
      var messagesInRDD = 0L
      rdd.asInstanceOf[HasOffsetRanges].offsetRanges.foreach(range => {
        debug("Got RDD " + rdd.id + " from Topic :: "
          + range.topic + " , partition :: " + range.partition + " messages Count :: " + range.count + " Offsets From :: "
          + range.fromOffset + " To :: " + range.untilOffset)
        messagesInRDD = inc(messagesInRDD, range.count())
      })
      if (messagesInRDD > 0L) {
        info(s"Got RDD ${rdd.id} with Partitions :: " + rdd.partitions.length + " and Messages Cnt:: " + messagesInRDD + " Executing Asynchronously Each of Them.")
        rdd foreachPartitionAsync (dataItr => {
          if (dataItr.nonEmpty) {
            val message = dataItr.next()._2
            val delim = if (message contains "\r\n") "\r\n" else "\n"
            val insuranceIds = new ArrayBuffer[String]
            Try(message.split(delim)) match {
              case Success(splitted) =>
                splitted.foreach(segment => {
                  if (segment.startsWith(IN1)) {
                    val retVal = splitAndReturn(segment, "\\|", 36, "policy_num")
                    if (retVal != null) insuranceIds += retVal
                  }
                })
              case Failure(t) => error(s"Failed to split message: $t")
            }
            if (insuranceIds.nonEmpty) {
              insuranceIds.foreach(id => {
                if (id.nonEmpty) {
                  info(s"id: $id")
                  // Filter insurance IDs
                  // Write message to file or send?
                  //                    info(s"Message: $message")
                }
              })
            }
          }
        })
      }
    })
  }

  def splitAndReturn(segment: String, delimiter: String, returnIndex: Int, segInfo: String): String = {
    info(s"segment: $segment")
    Try(segment.split(delimiter)(returnIndex)) match {
      case Success(id) =>
        info(s"Found $segInfo: $id")
        id
      case Failure(t) =>
        warn(s"No $segInfo for segment")
        null
    }
  }

//  def splitAndReturn2(segment: String, delimiter: String, returnIndex: Int, segInfo: String): Option[String] = {
//    info(s"segment: $segment")
//    try {
//      Some(segment.split(delimiter)(returnIndex))
//    } catch {
//      case e: Exception => None
//    }
////    Try(segment.split(delimiter)(returnIndex)) match {
////      case Success(id) =>
////        info(s"Found $segInfo: $id")
////        id
////      case Failure(t) =>
////        warn(s"No $segInfo for segment")
////        None
////    }
//  }

  private def startStreams() = {
    try {
      sparkStrCtx start()
      info(s"Started Spark Streaming Context Execution :: ${new Date()}")
      sparkStrCtx awaitTermination()
    } catch {
      case t: Throwable => error("Spark Context Starting Failed will try with Retry Policy", t)

    } finally {
      info("finally")
    }
  }

}