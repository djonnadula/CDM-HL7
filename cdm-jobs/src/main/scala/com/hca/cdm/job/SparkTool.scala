package com.hca.cdm.job

import java.util.Date
import java.util.concurrent.atomic.AtomicBoolean
import java.lang.System.{getenv => fromEnv}
import com.hca.cdm.kafka.config.HL7ConsumerConfig.{createConfig => consumerConf}
import com.hca.cdm._
import com.hca.cdm.log.Logg
import com.hca.cdm.spark.{Hl7SparkUtil => sparkUtil}
import com.hca.cdm.utils.RetryHandler
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.streaming.StreamingContext
import scala.collection.JavaConverters._

/**
  * Created by dof7475 on 7/5/2017.
  */
trait SparkTool extends Logg {

  self =>
   val defaultPar = lookUpProp("spark.default.parallelism")
   val batchCycle = lookUpProp("batch.interval").toInt
   val batchDuration = sparkUtil batchCycle(lookUpProp("batch.time.unit"), batchCycle)
   val checkPoint = lookUpProp("checkpoint")
   val sparkConf = sparkUtil.getConf(lookUpProp("app"), defaultPar)
   val restoreFromChk = new AtomicBoolean(true)
   val fileSystem = FileSystem.get(new Configuration())
   val app = lookUpProp("app")
   val checkpointEnable = lookUpProp("spark.checkpoint.enable").toBoolean
   val maxPartitions = defaultPar.toInt * lookUpProp("spark.dynamicAllocation.maxExecutors").toInt
   val consumerGroup = lookUpProp("group")
   val kafkaConsumerProp = (consumerConf(consumerGroup) asScala) toMap
   val topicsToSubscribe = Set(lookUpProp("kafka.source"))
   val appHomeDir = fileSystem.getHomeDirectory.toString
   val stagingDir = fromEnv("SPARK_YARN_STAGING_DIR")
   val mqQueue = enabled(lookUpProp("mq.queueResponse"))

  var sparkStrCtx: StreamingContext = initContext

  def newCtxIfNotExist = new (() => StreamingContext) {
    override def apply(): StreamingContext = {
      val ctx = sparkUtil createStreamingContext(sparkConf, batchDuration)
      info(s"New Checkpoint Created for $app $ctx")
      runJob(ctx)
      ctx
    }
  }

  def initContext: StreamingContext = {
    sparkStrCtx = if (checkpointEnable) sparkUtil streamingContext(checkPoint, newCtxIfNotExist) else sparkUtil createStreamingContext(sparkConf, batchDuration)
    sparkStrCtx.sparkContext setJobDescription lookUpProp("job.desc")
    if (!checkpointEnable) runJob(sparkStrCtx)
    sparkStrCtx
  }

  /**
    * Starts Spark Streaming
    */
  def startStreams() = {
    try {
      sparkStrCtx start()
      info(s"Started Spark Streaming Context Execution :: ${new Date()}")
      sparkStrCtx awaitTermination()
    } catch {
      case t: Throwable => error("Spark Context Starting Failed will try with Retry Policy", t)
        val retry = RetryHandler()

        def retryStart(): Unit = {
          sparkStrCtx start()
          info(s"Started Spark Streaming Context Execution :: ${new Date()}")
          sparkStrCtx awaitTermination()
        }

        tryAndLogErrorMes(retry.retryOperation(retryStart), error(_: Throwable), Some(s"Cannot Start sparkStrCtx for $app After Retries ${retry.triesMadeSoFar()}"))

    } finally {
      shutDown()
      close()
    }
  }

  def runJob(sparkStrCtx: StreamingContext): Unit = {}

  /**
    * Shutdowns Spark context and Streaming Context Gracefully allowing already Executing tasks to be Completed
    */
  def shutdownEverything(sparkStrCtx: StreamingContext): Unit = if (sparkStrCtx != null) sparkStrCtx stop(stopSparkContext = true, stopGracefully = true)

  def closeResource(res: AutoCloseable): Unit = if (res != null) res.close()

  def shutDown(): Unit = {
    sparkUtil shutdownEverything sparkStrCtx
    closeResource(fileSystem)
  }

  /**
    * Close All Resources
    */
  def close() = {
    info(s"Shutdown Invoked for $app")
    info(s"Shutdown Completed for $app")
  }
}
