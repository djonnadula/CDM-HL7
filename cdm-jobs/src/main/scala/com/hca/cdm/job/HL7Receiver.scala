package com.hca.cdm.job


import java.lang.System.{getenv => fromEnv}
import java.util.Date
import java.util.concurrent.atomic.AtomicBoolean
import com.hca.cdm.Models.MSGMeta
import scala.Int.MaxValue
import com.hca.cdm._
import com.hca.cdm.spark.receiver.{MqReceiver => receiver}
import com.hca.cdm.hadoop.OverSizeHandler
import com.hca.cdm.hl7.audit.AuditConstants._
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
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import com.hca.cdm.hl7.constants.HL7Constants._
import com.hca.cdm.hl7.constants.HL7Types.{withName => hl7}
import scala.collection.mutable.ListBuffer
import scala.language.postfixOps

/**
  * Created by Devaraj Jonnadula on 12/14/2016.
  */
object HL7Receiver extends Logg with App {

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
  private val batchDuration = {
    lookUpProp("hl7.batch.time.unit") match {
      case "seconds" => Seconds(batchCycle)
      case "ms" => Milliseconds(batchCycle)
      case _ => Seconds(batchCycle)
    }
  }
  private val numberOfReceivers = lookUpProp("hl7.spark.dynamicAllocation.minExecutors").toInt + 1
  printConfig()

  // ******************************************************** Job Part ***********************************************
  private var sHook: Thread = _
  private val app = lookUpProp("hl7.app")
  private val jobDesc = lookUpProp("job.desc")
  private val rejectedTopic = lookUpProp("hl7.reject")
  private val auditTopic = lookUpProp("hl7.audit")
  private val kafkaProducerConf = producerConf()
  private val mqHosts = lookUpProp("mq.hosts")
  private val mqManager = lookUpProp("mq.manager")
  private val mqChannel = lookUpProp("mq.channel")
  private val mqPort = lookUpProp("mq.port").toInt
  private val tlmAck = {
    val tem = lookUpProp("mq.queueResponse")
    if (tem == EMPTYSTR || (tem ne null)) Some(tem)
    else None
  }
  private val maxMessageSize = lookUpProp("hl7.message.max") toInt
  private val messageTypes = lookUpProp("hl7.messages.type") split COMMA
  private val hl7MsgMeta = messageTypes.map(mtyp => mtyp -> getReceiverMeta(hl7(mtyp), lookUpProp(s"$mtyp.wsmq.source"), lookUpProp(s"$mtyp.kafka"))).toMap
  private val hl7QueueMapping = hl7MsgMeta.map(x => x._2.wsmq -> x._1)
  private val hl7KafkaOut = hl7MsgMeta.map(x => x._1 -> x._2.kafka)
  private val hl7Queues = hl7MsgMeta.map(_._2.wsmq).toSet
  private val tlmAuditor = hl7MsgMeta map (x => x._2.wsmq -> (tlmAckMsg(x._1)(_: MSGMeta)))
  private val rawOverSized = OverSizeHandler(rawStage, lookUpProp("hl7.direct.raw"))
  private val rejectOverSized = OverSizeHandler(rejectStage, lookUpProp("hl7.direct.reject"))

  // ****************** Spark Part ***********************************************
  private val checkpointEnable = lookUpProp("hl7.spark.checkpoint.enable").toBoolean
  private val checkPoint = lookUpProp("hl7.checkpoint")
  private val restoreFromChk = new AtomicBoolean(true)
  private val sparkConf = sparkUtil.getConf(lookUpProp("hl7.app"), defaultPar, kafkaConsumer = false)
  if (checkpointEnable) {
    sparkConf.set("spark.streaming.receiver.writeAheadLog.enable", "true")
    restoreFromChk set false
  }
  private val newCtxIfNotExist = new (() => StreamingContext) {
    override def apply(): StreamingContext = {
      val ctx = sparkUtil createStreamingContext(sparkConf, batchDuration)
      restoreFromChk set false
      ctx
    }
  }
  private val sparkStrCtx: StreamingContext = if (checkpointEnable) sparkUtil streamingContext(checkPoint, newCtxIfNotExist) else sparkUtil createStreamingContext(sparkConf, batchDuration)
  sparkStrCtx.sparkContext setJobDescription lookUpProp("job.desc")
  initialise(sparkStrCtx)
  startStreams()

  private def initialise(sparkStrCtx: StreamingContext): Unit = {
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
    if (!restoreFromChk.get()) runJob(sparkStrCtx)
  }


  /**
    * Main Job Execution
    *
    * Executes Each RDD Partitions Asynchronously.
    */
  private def runJob(sparkStrCtx: StreamingContext): Unit = {
    val streamLine = sparkStrCtx.union((0 until numberOfReceivers).map(receiver => {
      val stream = sparkStrCtx.receiverStream(new receiver(receiver,app, jobDesc, mqHosts, mqPort, mqManager, mqChannel, batchCycle, batchRate, hl7Queues)(tlmAck, tlmAuditor, metaFromRaw(_: String)))
      info(s"WSMQ Stream Was Opened Successfully with ID :: ${stream.id} for Receiver $receiver")
      stream
    }))
    streamLine foreachRDD (rdd => {
      info(s"Got RDD ${rdd.id} with Partitions :: ${rdd.partitions.length} Executing Asynchronously Each of Them.")
      val rejectOut = rejectedTopic
      val auditOut = auditTopic
      val prodConf = kafkaProducerConf
      val tlmDest = tlmAck
      val mqHosts = this.mqHosts
      val mqManager = this.mqManager
      val mqChannel = this.mqChannel
      val mqPort = this.mqPort
      val app = this.app
      val confFile = config_file
      val maxMessageSize = this.maxMessageSize
      val hl7QueueMapping = this.hl7QueueMapping
      val hl7KafkaOut = this.hl7KafkaOut
      val tracker = new ListBuffer[FutureAction[Unit]]
      tracker += rdd foreachPartitionAsync (dataItr => {
        dataItr nonEmpty match {
          case true =>
            propFile = confFile
            val kafkaOut = KProducer(multiDest = true)(prodConf)
            val rawOut = kafkaOut.writeData(_: String, _: String, _: String)(maxMessageSize, rawOverSized)
            val auditIO = kafkaOut.writeData(_: String, _: String, auditOut)(MaxValue)
            val audit = auditMsg(_: String, rawStage)(EMPTYSTR, _: MSGMeta)
            val hl7RejIO = kafkaOut.writeData(_: String, _: String, rejectOut)(maxMessageSize, rejectOverSized)
            dataItr foreach { mqData =>
              hl7QueueMapping isDefinedAt mqData.source match {
                case true =>
                  val hl7Str = hl7QueueMapping(mqData.source)
                  if (tryAndLogThr(rawOut(mqData.data, header(hl7Str, rawStage, Left(mqData.msgMeta)), hl7KafkaOut(hl7Str)), s"$hl7Str$COLON$hl7RawIOFun", error(_: Throwable))) {
                    tryAndLogThr(auditIO(audit(hl7Str, mqData.msgMeta), header(hl7Str, auditHeader, Left(mqData.msgMeta))), s"$hl7Str$COLON$hl7RawAuditIOFun", error(_: Throwable))
                  }
                  else {
                    val msg = rejectMsg(hl7Str, rawStage, mqData.msgMeta, " Writing Data to OUT Failed ", null, null, mqData.data)
                    tryAndLogThr(hl7RejIO(msg, header(hl7Str, rejectStage, Left(mqData.msgMeta))), s"$hl7Str$COLON hl7RejIO-rejectMsg", error(_: Throwable))
                    error(s"Sending Raw to Kafka Failed :: $msg")
                  }
                case _ =>
                  val msg = rejectRawMsg(mqData.source, rawStage, mqData.data, s"Cannot Deal with HL7 Came in $mqData.source . Only Activated these MessageTypes  ${hl7QueueMapping.values.mkString(COMMA)}", null, stackTrace = false)
                  tryAndLogThr(hl7RejIO(msg, header(mqData.source, rejectStage, Left(mqData.msgMeta))), s"${mqData.source}$COLON rejectRawMsg", error(_: Throwable))
              }
            }
          case _ => info(s"Partition was Empty For RDD So skipping $dataItr for RDD ${rdd.id}")
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
      case t: Throwable => error("Spark Context Starting Failed ", t)
    } finally {
      close()
    }
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


