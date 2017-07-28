package com.hca.cdm.job

import java.io.{BufferedOutputStream, BufferedWriter, OutputStream}
import java.lang.System.{getenv => fromEnv}
import java.util.Date

import com.hca.cdm.Models.MSGMeta
import com.hca.cdm._
import com.hca.cdm.hl7.audit.AuditConstants._
import com.hca.cdm.hl7.audit._
import com.hca.cdm.hl7.constants.HL7Types._
import com.hca.cdm.spark.receiver.{MqReceiver => receiver}
import com.hca.cdm.hl7.model._
import com.hca.cdm.log.Logg
import com.hca.cdm.mq.WsmqConsumer
import com.hca.cdm.spark.{Hl7SparkUtil => sparkUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.streaming.StreamingContext
import com.hca.cdm.hadoop._
import scala.language.postfixOps



/**
  * Created by dof7475 on 7/24/2017.
  */
object SimpleMQConsumer extends Logg with App {

  self =>
  private lazy val config_file = args(0)
  info("config_file: " + config_file)
  propFile = config_file
  reload(propFile)
  private lazy val app = lookUpProp("hl7.app")
  private lazy val jobDesc = lookUpProp("job.desc")
  private lazy val defaultPar = lookUpProp("hl7.spark.default.parallelism")
  private lazy val sparkConf = sparkUtil.getConf(lookUpProp("hl7.app"), defaultPar)
  private lazy val batchCycle = lookUpProp("hl7.batch.interval").toInt
  private lazy val batchRate = lookUpProp("hl7.batch.rate").toInt
  private lazy val maxPartitions = defaultPar.toInt * lookUpProp("hl7.spark.dynamicAllocation.maxExecutors").toInt
  private lazy val batchDuration = sparkUtil batchCycle(lookUpProp("hl7.batch.time.unit"), batchCycle)
  private lazy val consumerGroup = lookUpProp("simpleMQconsumer.group")
  private lazy val checkpointEnable = lookUpProp("simpleMQconsumer.spark.checkpoint.enable").toBoolean
  private lazy val checkPoint = lookUpProp("simpleMQconsumer.checkpoint")
  private lazy val fileSystem = FileSystem.get(new Configuration())
  private lazy val appHomeDir = fileSystem.getHomeDirectory.toString
  private lazy val stagingDir = fromEnv("SPARK_YARN_STAGING_DIR")
  private lazy val numberOfReceivers = Range(0, lookUpProp("hl7.spark.dynamicAllocation.minExecutors").toInt)
  private lazy val mqQueue = lookUpProp("mq.queue")
  private lazy val mqHosts = lookUpProp("mq.hosts")
  private lazy val mqManager = lookUpProp("mq.manager")
  private lazy val mqChannel = lookUpProp("mq.channel")
  private lazy val mqPort = lookUpProp("mq.port").toInt
  private lazy val path = lookUpProp("hdfs.path")

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

  private def runJob(sparkStrCtx: StreamingContext): Unit = {
    val stream = if (numberOfReceivers.size == 1) sparkStrCtx.receiverStream( new WsmqConsumer(app, mqHosts, mqPort, mqManager, mqChannel, mqQueue))
    else {
      sparkStrCtx.union(numberOfReceivers.map(id => {
        val stream = sparkStrCtx.receiverStream(new WsmqConsumer(app, mqHosts, mqPort, mqManager, mqChannel, mqQueue))
        info(s"WSMQ Stream Was Opened Successfully with ID :: ${stream.id} for Receiver $id")
        stream
      }))
    }
    stream foreachRDD (rdd => {
      info(s"Got RDD ${rdd.id} with Partitions :: ${rdd.partitions.length} Executing Asynchronously Each of Them.")
      val confFile = config_file
      rdd foreachPartitionAsync (dataItr => {
        if (dataItr.nonEmpty) {
          propFile = confFile
          dataItr foreach { mqdata =>
            writeObject(mqdata, mqQueue + "-mq-data", path, fileSystem.getConf)
            info("mqdata.data: " + mqdata)
          }
        }
      })
    })
  }



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
