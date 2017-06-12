package com.hca.cdm.job.fieldops

import java.lang.System.{getenv => fromEnv}
import java.util.Date

import com.hca.cdm._
import com.hca.cdm.job.HL7Job._
import com.hca.cdm.kafka.config.HL7ConsumerConfig.{createConfig => consumerConf}
import com.hca.cdm.log.Logg
import com.hca.cdm.spark.{Hl7SparkUtil => sparkUtil}
import com.hca.cdm.utils.RetryHandler
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.apache.spark.streaming.StreamingContext

import scala.collection.JavaConverters._

/**
  * Created by dof7475 on 6/1/2017.
  */
object EastFloridaMedAdminJob extends Logg with App {

  self =>
  private val config_file = args(0)
  info("config_file: " + config_file)
  propFile = config_file
  private val app = lookUpProp("hl7.app")
  private val checkpointEnable = lookUpProp("EFMedAdmin.spark.checkpoint.enable").toBoolean
  private val checkPoint = lookUpProp("EFMedAdmin.checkpoint")
  private val defaultPar = lookUpProp("hl7.spark.default.parallelism")
  private val sparkConf = sparkUtil.getConf(lookUpProp("hl7.app"), defaultPar)
  private val batchCycle = lookUpProp("hl7.batch.interval").toInt
  private val maxPartitions = defaultPar.toInt * lookUpProp("hl7.spark.dynamicAllocation.maxExecutors").toInt
  private val batchDuration = sparkUtil batchCycle(lookUpProp("hl7.batch.time.unit"), batchCycle)
  private val consumerGroup = lookUpProp("EFMedAdmin.group")
  private val kafkaConsumerProp = (consumerConf(consumerGroup) asScala) toMap
  private val topicsToSubscribe = Set(lookUpProp("EFMedAdmin.kafka.source"))
  private val dbUser = lookUpProp("EFMedAdmin.db.user")
  private val dbPwd = lookUpProp("EFMedAdmin.db.pwd")
  private val dbURL = lookUpProp("EFMedAdmin.db.url")
  private val dbName = lookUpProp("EFMedAdmin.db.name")
  private val fileSystem = FileSystem.get(new Configuration())
  private val appHomeDir = fileSystem.getHomeDirectory.toString
  private val stagingDir = fromEnv("SPARK_YARN_STAGING_DIR")

  //reload(config_file, Some(fileSystem.open(new Path(appHomeDir + FS + stagingDir + FS + config_file))))
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
    val streamLine = sparkUtil stream(sparkStrCtx, kafkaConsumerProp, topicsToSubscribe)
    info("kafkaConsumerProp: " + kafkaConsumerProp)
    info("subscribed topics: " + topicsToSubscribe.mkString(","))
    info(s"Kafka Stream Was Opened Successfully with ID :: ${streamLine.id}")
    streamLine foreachRDD (rdd =>
      {
        info("in rdd zone")
        var messagesInRDD = 0L
        rdd.asInstanceOf[HasOffsetRanges].offsetRanges.foreach(range => {
          debug("Got RDD " + rdd.id + " from Topic :: "
            + range.topic + " , partition :: " + range.partition + " messages Count :: " + range.count + " Offsets From :: "
            + range.fromOffset + " To :: " + range.untilOffset)
          messagesInRDD = inc(messagesInRDD, range.count())
        })
        if (messagesInRDD > 0L) {
          info(s"Got RDD ${rdd.id} with Partitions :: " + rdd.partitions.length + " and Messages Cnt:: " + messagesInRDD + " Executing Asynchronously Each of Them.")
          info(s"rdd: ${rdd.toString()}")
          rdd.collect().foreach(x => println("collected rdd: " + x))
          val ssqlCtx = new SQLContext(rdd.sparkContext)
          val dataFrame = ssqlCtx.createDataFrame(rdd)
          val f = dataFrame.first()
          info(s"f: $f")
        }
      }
     )
    //TODO: write data to sql-server or wsmq
  }

  case class MedAdmin(message_control_id: String, sending_facility: String, patient_account_num: String, medical_record_num: String,
                      medical_record_urn: String, placer_order_num: String, filler_order_num: String, administration_sub_id_counter: String,
                      patient_type: String, date_time_end_of_administration: String, date_time_start_of_administration: String,
                      administration_site: String, administered_dosage_form_id: String, administered_units: String, indication_text: String,
                      indication_id: String, administering_provider: String)

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
