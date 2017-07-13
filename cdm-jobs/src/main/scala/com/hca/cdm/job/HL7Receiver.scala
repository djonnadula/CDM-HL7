package com.hca.cdm.job


import java.lang.System.{getenv => fromEnv}
import java.util.Date
import com.hca.cdm.Models.MSGMeta
import scala.Int.MaxValue
import com.hca.cdm._
import com.hca.cdm.spark.receiver.{MqReceiver => receiver}
import com.hca.cdm.hadoop.OverSizeHandler
import com.hca.cdm.hl7.audit._
import com.hca.cdm.hl7.model._
import com.hca.cdm.kafka.config.HL7ProducerConfig.{createConfig => producerConf}
import com.hca.cdm.kafka.producer.{KafkaProducerHandler => KProducer}
import com.hca.cdm.kafka.util.TopicUtil.{createTopicIfNotExist => createTopic}
import com.hca.cdm.log.Logg
import com.hca.cdm.spark.{Hl7SparkUtil => sparkUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.FutureAction
import org.apache.spark.streaming.StreamingContext
import com.hca.cdm.hl7.constants.HL7Constants._
import com.hca.cdm.hl7.constants.HL7Types.{withName => hl7}
import org.apache.spark.deploy.SparkHadoopUtil.{get => hdpUtil}
import scala.collection.mutable.ListBuffer
import scala.language.postfixOps
import AuditConstants._
import com.hca.cdm.auth.LoginRenewer
import com.hca.cdm.auth.LoginRenewer.loginFromKeyTab
import com.hca.cdm.utils.RetryHandler


/**
  * Created by Devaraj Jonnadula on 12/14/2016.
  */
object HL7Receiver extends Logg with App {

  self =>
  private val config_file = args(0)
  propFile = config_file
  private val fileSystem = FileSystem.get(new Configuration())
  private val appHomeDir = fileSystem.getHomeDirectory.toString
  private val stagingDir = fromEnv("SPARK_YARN_STAGING_DIR")
  private val it = fileSystem.listFiles(new Path(appHomeDir + FS + stagingDir), true)
  while (it.hasNext) info("File uploaded For Job :: " + it.next().getPath.toString)
  reload(config_file, Some(fileSystem.open(new Path(appHomeDir + FS + stagingDir + FS + config_file))))
  private val defaultPar = lookUpProp("hl7.spark.default.parallelism")
  private val batchCycle = lookUpProp("hl7.batch.interval").toInt
  private val batchRate = lookUpProp("hl7.batch.rate").toInt
  private val batchDuration = sparkUtil batchCycle(lookUpProp("hl7.batch.time.unit"), batchCycle)
  private val numberOfReceivers = Range(0, lookUpProp("hl7.spark.dynamicAllocation.minExecutors").toInt)
  printConfig()

  // ******************************************************** Job Part ***********************************************
  private var sHook: Thread = _
  private val app = lookUpProp("hl7.app")
  private val jobDesc = lookUpProp("job.desc")
  private val rejectedTopic = lookUpProp("hl7.reject")
  private val auditTopic = lookUpProp("hl7.audit")
  private val kafkaProducerConf = producerConf()
  private val maxMessageSize = lookUpProp("hl7.message.max") toInt
  private val messageTypes = lookUpProp("hl7.messages.type") split COMMA
  private val hl7MsgMeta = messageTypes.map(mtyp => mtyp -> getReceiverMeta(hl7(mtyp), lookUpProp(s"$mtyp.wsmq.source"), lookUpProp(s"$mtyp.kafka"))).toMap
  private val hl7QueueMapping = hl7MsgMeta.map(x => x._2.wsmq -> x._1)
  private val hl7KafkaOut = hl7MsgMeta.map(x => x._1 -> x._2.kafka)
  private val hl7Queues = hl7MsgMeta.map(_._2.wsmq).toSet
  private val tlmAuditor = hl7MsgMeta map (x => x._2.wsmq -> (tlmAckMsg(x._1, applicationSending, WSMQ, HDFS)(_: MSGMeta)))
  private val rawOverSized = OverSizeHandler(rawStage, lookUpProp("hl7.direct.raw"))
  private val rejectOverSized = OverSizeHandler(rejectStage, lookUpProp("hl7.direct.reject"))
  initialise()

  // ****************** Spark Part ***********************************************
  private val checkpointEnable = lookUpProp("hl7.spark.checkpoint.enable").toBoolean
  private val walEnabled = lookUpProp("hl7.spark.wal.enable").toBoolean
  private val checkPoint = lookUpProp("hl7.checkpoint")
  private val sparkConf = sparkUtil.getConf(lookUpProp("hl7.app"), defaultPar, kafkaConsumer = false)
  if (walEnabled) {
    sparkConf.set("spark.streaming.receiver.writeAheadLog.enable", "true")
    sparkConf.set("spark.streaming.receiver.writeAheadLog.maxFailures", "30")
    // sparkConf.set("spark.streaming.receiver.writeAheadLog.rollingIntervalSecs","3600")
    sparkConf.set("spark.streaming.driver.writeAheadLog.allowBatching", "true")
    sparkConf.set("spark.streaming.driver.writeAheadLog.batchingTimeout", "20000")
    sparkConf.set("spark.streaming.receiver.blockStoreTimeout", "180")
  }
  if (checkpointEnable) {
    sparkConf.set("spark.streaming.driver.writeAheadLog.maxFailures", "30")
    sparkConf.set("spark.streaming.driver.writeAheadLog.batchingTimeout", "180")
  }
  if (lookUpProp("hl7.batch.time.unit") == "ms") {
    sparkConf.set("spark.streaming.blockInterval", (batchCycle / 2).toString)
  }

  private def newCtxIfNotExist = new (() => StreamingContext) {
    override def apply(): StreamingContext = {
      val ctx = sparkUtil createStreamingContext(sparkConf, batchDuration)
      info(s"New Checkpoint Created for $app $ctx")
      ctx.remember(batchDuration * 2)
      runJob(ctx)
      ctx
    }
  }
  private val hdpConf = hdpUtil.conf
  private var sparkStrCtx: StreamingContext = initContext
  startStreams()

  private def initContext: StreamingContext = {
    sparkStrCtx = if (checkpointEnable) sparkUtil streamingContext(checkPoint, newCtxIfNotExist) else sparkUtil createStreamingContext(sparkConf, batchDuration)
    sparkStrCtx.sparkContext setJobDescription lookUpProp("job.desc")
    hdpConf.set("hadoop.security.authentication", "Kerberos")
    loginFromKeyTab(sparkConf.get("spark.yarn.keytab"), sparkConf.get("spark.yarn.principal"), Some(hdpUtil.conf))
    LoginRenewer.scheduleRenewal(master = true)
    if (!checkpointEnable) runJob(sparkStrCtx)
    sparkStrCtx
  }

  /**
    * Main Job Execution
    *
    * Executes Each RDD Partitions Asynchronously.
    */
  private def runJob(sparkStrCtx: StreamingContext): Unit = {
    val stream = if (numberOfReceivers.size == 1) sparkStrCtx.receiverStream(new receiver(sparkStrCtx.sparkContext.getConf.get("spark.yarn.access.namenodes"), 0, app, jobDesc, batchDuration.milliseconds.toInt, batchRate, hl7Queues)(tlmAuditor, metaFromRaw(_: String), rawStage))
    else {
      sparkStrCtx.union(numberOfReceivers.map(id => {
        val stream = sparkStrCtx.receiverStream(new receiver(sparkStrCtx.sparkContext.getConf.get("spark.yarn.access.namenodes"), id, app, jobDesc, batchDuration.milliseconds.toInt, batchRate, hl7Queues)(tlmAuditor, metaFromRaw(_: String), rawStage))
        info(s"WSMQ Stream Was Opened Successfully with ID :: ${stream.id} for Receiver $id")
        stream
      }))
    }
    stream foreachRDD (rdd => {
      info(s"Got RDD ${rdd.id} with Partitions :: ${rdd.partitions.length} Executing Asynchronously Each of Them.")
      val rejectOut = self.rejectedTopic
      val auditOut = self.auditTopic
      val prodConf = self.kafkaProducerConf
      val confFile = self.config_file
      val maxMessageSize = self.maxMessageSize
      val hl7QueueMapping = self.hl7QueueMapping
      val hl7KafkaOut = self.hl7KafkaOut
      val rawOverSized = self.rawOverSized
      val rejectOverSized = self.rejectOverSized
      val tracker = new ListBuffer[FutureAction[Unit]]
      tracker += rdd foreachPartitionAsync (dataItr => {
        if (dataItr nonEmpty) {
          propFile = confFile
          val kafkaOut = KProducer()(prodConf)
          val rawOut = kafkaOut.writeData(_: String, _: String, _: String)(maxMessageSize, rawOverSized)
          val auditIO = kafkaOut.writeData(_: String, _: String, auditOut)(MaxValue)
          val audit = auditMsg(_: String, rawStage)(EMPTYSTR, _: MSGMeta)
          val hl7RejIO = kafkaOut.writeData(_: String, _: String, rejectOut)(maxMessageSize, rejectOverSized)
          dataItr foreach { mqData =>
            if (hl7QueueMapping isDefinedAt mqData.source) {
              val hl7Str = hl7QueueMapping(mqData.source)
              if (tryAndLogThr(rawOut(mqData.data, header(hl7Str, rawStage, Left(mqData.msgMeta)), hl7KafkaOut(hl7Str)), s"$hl7Str$COLON$hl7RawIOFun", error(_: Throwable))) {
                tryAndLogThr(auditIO(audit(hl7Str, mqData.msgMeta), header(hl7Str, auditHeader, Left(mqData.msgMeta))), s"$hl7Str$COLON$hl7RawAuditIOFun", error(_: Throwable))
              }
              else {
                val msg = rejectMsg(hl7Str, rawStage, mqData.msgMeta, " Writing Data to OUT Failed ", null, null, mqData.data)
                tryAndLogThr(hl7RejIO(msg, header(hl7Str, rejectStage, Left(mqData.msgMeta))), s"$hl7Str$COLON hl7RejIO-rejectMsg", error(_: Throwable))
                error(s"Sending Raw to Kafka Failed :: $msg")
              }
            } else {
              val msg = rejectRawMsg(mqData.source, rawStage, mqData.data, s"Cannot Deal with HL7 Came in $mqData.source . Only Activated these MessageTypes  ${hl7QueueMapping.values.mkString(COMMA)}", null, stackTrace = false)
              tryAndLogThr(hl7RejIO(msg, header(mqData.source, rejectStage, Left(mqData.msgMeta))), s"${mqData.source}$COLON rejectRawMsg", error(_: Throwable))
            }
          }
        } else {
          info(s"Partition was Empty For RDD So skipping $dataItr for RDD ${rdd.id}")
        }
      })
      tracker.foreach(_.get())
      info(s"Processing Completed for RDD :: ${rdd.id}")
    })
  }

  /**
    * Starts Spark Streaming
    */
  private def startStreams() = {
    try {
      sparkStrCtx start()
      info(s"Started Spark Streaming Context Execution :: ${new Date()}")
      sparkStrCtx awaitTermination()
    } catch {
      case t: Throwable =>
        if (!t.isInstanceOf[InterruptedException]) {
          error("Spark Context Starting Failed ", t)
          val retry = RetryHandler()

          def retryStart(): Unit = {
            sparkStrCtx start()
            info(s"Started Spark Streaming Context Execution :: ${new Date()}")
            sparkStrCtx awaitTermination()
          }

          tryAndLogErrorMes(retry.retryOperation(retryStart), error(_: Throwable), Some(s"Cannot Start sparkStrCtx for $app After Retries ${retry.triesMadeSoFar()}"))
        }
    } finally {
      close()
    }
  }

  private def initialise(): Unit = {
    info("Job Initialisation Started on :: " + new Date())
    createTopic(auditTopic, segmentPartitions = false)
    createTopic(rejectedTopic, segmentPartitions = false)
    hl7KafkaOut.foreach(topic => createTopic(topic._2, segmentPartitions = false))
    sHook = newThread(s"$app SparkCtx SHook", runnable({
      close()
      info(currThread.getName + " Shutdown HOOK Completed for " + app)
    }))
    registerHook(sHook)
    info("Initialisation Done. Running Job")
  }


  /**
    * Close All Resources
    */
  private def close() = {
    info(s"Shutdown Invoked for $app")
    sparkUtil shutdownEverything sparkStrCtx
    closeResource(fileSystem)
    info(s"Shutdown Completed for $app")
    info("*****************************************END***********************************************************")
  }


}


