package com.hca.cdm.job.fieldops

import com.hca.cdm._
import com.hca.cdm.log.Logg
import com.hca.cdm.spark.{Hl7SparkUtil => sparkUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
  * Created by dof7475 on 6/1/2017.
  */
object EastFloridaMedAdmin extends Logg with App {

  private val app = lookUpProp("efMedAdmin.app")
  private var sparkStrCtx: StreamingContext = initContext
  private val checkpointEnable = lookUpProp("efMedAdmin.spark.checkpoint.enable").toBoolean
  private val checkPoint = lookUpProp("efMedAdmin.checkpoint")
  private val defaultPar = lookUpProp("efMedAdmin.spark.default.parallelism")
  private val sparkConf = sparkUtil.getConf(lookUpProp("efMedAdmin.app"), defaultPar, kafkaConsumer = false)
  private val batchCycle = lookUpProp("efMedAdmin.batch.interval").toInt
  private val batchDuration = sparkUtil batchCycle(lookUpProp("efMedAdmin.batch.time.unit"), batchCycle)

  def createStreamingContext(conf: SparkConf, timeUnit: Duration): StreamingContext = new StreamingContext(conf, timeUnit)

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
    //TODO: read data from a Kafka topio

    //TODO: write data to sql-server
  }

}
