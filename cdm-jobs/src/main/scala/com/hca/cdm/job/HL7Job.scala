package com.hca.cdm.job

import com.hca.cdm._
import com.hca.cdm.hl7.audit.AuditConstants._
import com.hca.cdm.hl7.constants.HL7Constants._
import com.hca.cdm.hl7.constants.HL7Types.{HL7, withName => hl7}
import com.hca.cdm.hl7.model._
import com.hca.cdm.hl7.parser.HL7Parser
import com.hca.cdm.kafka.config.HL7ConsumerConfig.{createConfig => conf}
import com.hca.cdm.kafka.producer.{KafkaProducerHandler => KProducer}
import com.hca.cdm.log.Logg
import com.hca.cdm.spark.config.{Hl7SparkUtil => sp}
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.launcher.SparkLauncher._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.language.postfixOps
import com.hca.cdm.hl7.audit._
import outStream.{println => console}
import com.hca.cdm.utils.DateUtil.{currentTimeStamp => timeStamp}

import scala.util.{Failure, Success, Try}

/**
  * Created by Devaraj Jonnadula on 8/19/2016.
  */
object HL7Job extends Logg with App {

  args length match {
    case 1 => reload(args(0))
      console("******************************************************************************************")
      console("***************** !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! **********************")
      console("***************** !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! **********************")
      console("**************************HCA CDM HL7 Processing System Initiated ************************")
      console("***************** !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! **********************")
      console("***************** !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! **********************")
    case _ =>
      console("******************************************************************************************")
      console("***************** !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! **********************")
      console("*****************          NOT FOUND - configuration file:          **********************")
      console("                                " + args + "        ")
      console("***************** !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! **********************")
      console("******************************************************************************************")
      console("*****************                Job terminated                ***********************")
      console("*******************************************************************************************")
      System.exit(-8)
  }
  printConfig()
  private val app = loopUpProp("hl7.app")
  private val appHome = loopUpProp("hl7.app.home")
  private val rejectedTopic = loopUpProp("hl7.reject")
  private val hl7JsonTopic = loopUpProp("hl7.json")
  private val segTopic = loopUpProp("hl7.segment")
  private val auditTopic = loopUpProp("hl7.audit")
  private val consumerGroup = loopUpProp("hl7.group")
  private val batchCycle = loopUpProp("hl7.batch.interval")
  private val defaultPar = loopUpProp("hl7.spark.default.parallelism")
  private val checkPointHome = loopUpProp("hl7.checkpoint")
  private val hl7MsgMeta = loopUpProp("hl7.messages.type").split(",").toList.map(mtyp => getMsgTypeMeta(hl7(mtyp)))
  private val templatesMapping = hl7MsgMeta.map(msgType => msgType.msgType -> loadTemplate(msgType)).toMap
  private val modelsForHl7 = hl7MsgMeta.map(msgType => msgType.msgType -> segmentsForHl7Type(msgType.msgType, loadSegments(msgType))).toMap
  private val jsonAuditor = hl7MsgMeta.map(msgType => msgType.msgType -> (auditMsg(msgType.msgType.toString, jsonStage)(EMPTYSTR, _: MSGMeta))).toMap
  private val segmentsAuditor = hl7MsgMeta.map(msgType => msgType.msgType -> (auditMsg(msgType.msgType.toString, segmentStage)(_: String, _: MSGMeta))).toMap
  private val allSegmentsInHl7Auditor = hl7MsgMeta.map(msgType => msgType.msgType -> (auditMsg(msgType.msgType.toString, segmentsInHL7)(_: String, _: MSGMeta))).toMap
  private val adhocAuditor = hl7MsgMeta.map(msgType => msgType.msgType -> (auditMsg(msgType.msgType.toString, adhocStage)(_: String, _: MSGMeta))).toMap
  private val registeredSegmentsForHl7 = modelsForHl7.mapValues(_.models.keySet)

  // ******************************************************** Spark Part ***********************************************

  private val hl7_spark_driver_maxResultSize = loopUpProp("hl7.spark.driver.maxResultSize")
  private val hl7_spark_dynamicAllocation_enabled = loopUpProp("hl7.spark.dynamicAllocation.enabled")
  private val hl7_spark_dynamicAllocation_minExecutors = loopUpProp("hl7.spark.dynamicAllocation.minExecutors")
  private val hl7_spark_dynamicAllocation_maxExecutors = loopUpProp("hl7.spark.dynamicAllocation.maxExecutors")
  private val hl7_spark_queue = loopUpProp("hl7.spark.queue")
  private val hl7_spark_master = loopUpProp("hl7.spark.master")
  private val hl7_spark_deploy_mode = loopUpProp("hl7.spark.deploy-mode")
  private val hl7_spark_num_executors = loopUpProp("hl7.spark.num-executors")
  private val hl7_spark_driver_memory = loopUpProp("hl7.spark.driver-memory")
  private val hl7_spark_executor_memory = loopUpProp("hl7.spark.executor-memory")

  val sparkLauncher = new SparkLauncher().setAppName(app)
    .setMaster(hl7_spark_master).setDeployMode(hl7_spark_deploy_mode).setVerbose(true).
    setConf(EXECUTOR_MEMORY, hl7_spark_executor_memory).setConf(EXECUTOR_CORES, defaultPar)
    .setConf("spark.driver-memory", hl7_spark_driver_memory)
    .setConf("spark.num-executors", hl7_spark_num_executors)
    .setConf("spark.queue", hl7_spark_queue)
    .setConf("spark.dynamicAllocation.maxExecutors", hl7_spark_dynamicAllocation_minExecutors)
    .setConf("spark.dynamicAllocation.minExecutors", hl7_spark_dynamicAllocation_minExecutors)
    .setConf("spark.driver.maxResultSize", hl7_spark_driver_maxResultSize)
    .setConf("spark.driver.maxResultSize", hl7_spark_driver_maxResultSize)
    .setMainClass(loopUpProp("hl7.class"))
    .setAppResource(loopUpProp("hl7.artifact"))
    .setSparkHome(loopUpProp("spark.home")).setConf("spark.driver.allowMultipleContexts", "true")
  val job = sparkLauncher.startApplication()


  private val kafkaOut = KProducer(auditTopic)
  private val hl7JsonIO = kafkaOut writeData(_: String, _: String, hl7JsonTopic)
  private val hl7RejIO = kafkaOut writeData(_: String, _: String, rejectedTopic)
  private val hl7SegIO = kafkaOut writeData(_: String, _: String, segTopic)
  private val auditIO = kafkaOut writeData(_: String, _: String, auditTopic)
  private val adhocIO = kafkaOut writeData(_: String, _: String, _: String)
  private val sparkConf = sp getConf(app, defaultPar)
  sparkConf.setMaster(hl7_spark_master)
  private val sparkStrCtx = sp getStreamingContext(batchCycle, sparkConf)
  private val checkPointInterval = Seconds(5)
  private val kafkaConsumerProp = conf(consumerGroup) asScala
  private val hl7Parsers = templatesMapping map (hl7 => hl7._1 -> new HL7Parser(hl7._2))
  private val segmentsModeler = modelsForHl7 map (hl7 =>
    hl7._1 -> new DataModelHandler(hl7._2, registeredSegmentsForHl7(hl7._1), segmentsAuditor(hl7._1), allSegmentsInHl7Auditor(hl7._1), adhocAuditor(hl7._1))(hl7SegIO, hl7RejIO, auditIO, adhocIO))
  private val segmentsMetrics = {
    val temp = new mutable.HashMap[HL7, SegmentsHandler]
    segmentsModeler foreach (x => temp += x)
    temp
  }
  private val sHook: Thread = newThread(app + consumerGroup + "SparkCtx SHook", runnable({
    sparkStrCtx.stop(stopSparkContext = true, stopGracefully = true)
    segmentsModeler.values foreach (_.shutDown())
    sleep(5000)
    closeResource(kafkaOut)
    job stop()
    info(currThread.getName + " Shutdown HOOK Completed")
  }))
  registerHook(sHook)
  runJob()


  def noAction(k: String, v: String) = {}

  private def runJob(): Unit = {

    hl7MsgMeta foreach (hl7Meta => {
      try {
        val hl7Str = hl7Meta.msgType.toString
        val streamLine = sp stream(sparkStrCtx, kafkaConsumerProp.toMap, hl7Meta.kafka)
        streamLine.foreachRDD(rdd => {
          rdd foreachPartitionAsync (hlsItr => {
            hlsItr foreach (hl7 => {
              Try(hl7Parsers(hl7Meta.msgType) transformHL7 hl7._2 rec) match {
                case Success(data) => data match {
                  case Left(out) =>
                    val meta = msgMeta(out._2)
                    if (tryAndLogErrorMes(hl7JsonIO(out._1,  hl7Str + COLON + jsonStage), error(_: String))) {
                      auditIO(jsonAuditor(hl7Meta.msgType)(meta), hl7Str + COLON + jsonStage)
                      segmentsModeler(hl7Meta.msgType).handleSegments(out._2, meta)
                    }
                    else {
                      val msg = rejectMsg(hl7Str, jsonStage, meta, " Writing Data to OUT Failed ", out._2)
                      hl7RejIO(msg, hl7 + COLON + hl7Str)
                      error("Sending JSON to Kafka Failed :: " + msg)
                    }
                  case Right(t) =>
                    val msg = rejectRawMsg(hl7Str, jsonStage, hl7._2, " Running Task FAILED ", t)
                    hl7RejIO(msg, hl7Str + COLON + rawStage)
                    error("Transforming Raw HL7 Failed :: " + msg)
                }
                case Failure(t) =>
                  val msg = rejectRawMsg(hl7Str, jsonStage, hl7._2, " Running Task FAILED ", t)
                  hl7RejIO(msg, hl7Str + COLON + rawStage)
                  error("Transforming Raw HL7 Failed :: " + msg)
              }
            })
          })
        })
      } catch {
        case t: Throwable => error(" HL7 Process failed for Message Type :: " + hl7Meta.msgType, t)
      }
    })
    try {
      sparkStrCtx.start()
      sparkStrCtx.awaitTermination()
    } catch {
      case t: Throwable => error("Spark Context Stating Failed ", t)
    } finally {
      unregister(sHook)
      shutdown(sparkStrCtx)
      info("Shutdown Completed")
      segmentsModeler.values.foreach(_.printStats())
      segmentsModeler.values.foreach(_.shutDown())
      sleep(5000)
      closeResource(kafkaOut)
      job.stop()
    }


  }


  private def shutdown(sparkStrCtx: StreamingContext) = {
    if (sparkStrCtx != null) {
      sparkStrCtx.stop(stopSparkContext = true, stopGracefully = true)

    }
  }

  private def getMsgTypeMeta(msgType: HL7) = {
    val msgTypeStr = msgType.toString
    MsgTypeMeta(msgType, appHome, loopUpProp(msgTypeStr + ".template"), loopUpProp(msgTypeStr + ".segments"),
      loopUpProp(msgTypeStr + ".kafka.source"))
  }

  /*

      /*private def registerSegmentsMetric(context: SparkContext, handler: SegmentsHandler) = {
        handler.metricsRegistry.map(met => {
          met._2.map(state => {
           val c = context.accumulable(0L, met._1 + COLON + state._1.toString)
          })
        })
      }*/




    }
  */

}
