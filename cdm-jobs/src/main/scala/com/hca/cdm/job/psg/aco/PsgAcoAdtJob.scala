package com.hca.cdm.job.psg.aco

import java.lang.Exception
import java.lang.System.{getenv => fromEnv}
import java.util.Date

import com.hca.cdm._
import com.hca.cdm.hl7.audit.AuditConstants._
import com.hca.cdm.hl7.audit._
import com.hca.cdm.kafka.config.HL7ConsumerConfig.{createConfig => consumerConf}
import com.hca.cdm.log.Logg
import com.hca.cdm.spark.{Hl7SparkUtil => sparkUtil}
import com.hca.cdm.hl7.constants.HL7Constants._
import com.hca.cdm.job.HL7Job._
import com.hca.cdm.mq.publisher.MQAcker
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.apache.spark.streaming.StreamingContext
import com.hca.cdm.job.psg.aco.PsgAcoAdtJobUtils._
import com.hca.cdm.kafka.config.HL7ProducerConfig._
import com.hca.cdm.kafka.producer.KafkaProducerHandler
import com.hca.cdm.kafka.util.TopicUtil._
import com.hca.cdm.utils.RetryHandler

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
  * Created by dof7475 on 6/1/2017.
  */
object PsgAcoAdtJob extends Logg with App {

  self =>
  private lazy val config_file = args(0)
  info("config_file: " + config_file)
  propFile = config_file
  reload(propFile)
  private lazy val app = lookUpProp("hl7.app")
  private lazy val defaultPar = lookUpProp("hl7.spark.default.parallelism")
  private lazy val sparkConf = sparkUtil.getConf(lookUpProp("hl7.app"), defaultPar)
  private lazy val batchCycle = lookUpProp("hl7.batch.interval").toInt
  private lazy val maxPartitions = defaultPar.toInt * lookUpProp("hl7.spark.dynamicAllocation.maxExecutors").toInt
  private lazy val batchDuration = sparkUtil batchCycle(lookUpProp("hl7.batch.time.unit"), batchCycle)
  private lazy val consumerGroup = lookUpProp("PSGACOADT.group")
  private lazy val checkpointEnable = lookUpProp("PSGACOADT.spark.checkpoint.enable").toBoolean
  private lazy val checkPoint = lookUpProp("PSGACOADT.checkpoint")
  private lazy val topicsToSubscribe = Set(lookUpProp("PSGACOADT.kafka.source"))
  private lazy val kafkaConsumerProp = (consumerConf(consumerGroup) asScala) toMap
  private lazy val fileSystem = FileSystem.get(new Configuration())
  private lazy val appHomeDir = fileSystem.getHomeDirectory.toString
  private lazy val stagingDir = fromEnv("SPARK_YARN_STAGING_DIR")
  private lazy val adtTypes = lookUpProp("PSGACOADT.adt.types").split(",")
  private lazy val insuranceFileLocation = new Path(lookUpProp("PSGACOADT.insurance.file.location"))
  private lazy val facFileLocation = new Path(lookUpProp("PSGACOADT.fac.file.location"))
  private lazy val insuranceArray = readFile(insuranceFileLocation, fileSystem)
  private lazy val facArray = readFile(facFileLocation, fileSystem)
  private lazy val outputFile = new Path(lookUpProp("PSGACOADT.output.file.location"))
  private lazy val mqQueue = enabled(lookUpProp("mq.queueResponse"))
  private lazy val insuranceNameMatcher = lookUpProp("PSGACOADT.insurance.name.match").split(",")
  private lazy val auditTopic = lookUpProp("hl7.audit")
  private lazy val maxMessageSize = lookUpProp("hl7.message.max") toInt
  private lazy val kafkaProducerConf = createConfig(auditTopic)

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
    //createTopicIfNotExist(auditTopic, segmentPartitions = false)
    sparkStrCtx
  }

  private def runJob(sparkStrCtx: StreamingContext): Unit = {
    val streamLine = sparkUtil stream(sparkStrCtx, kafkaConsumerProp, topicsToSubscribe)
    info("kafkaConsumerProp: " + kafkaConsumerProp)
    info("subscribed topics: " + topicsToSubscribe.mkString(","))
    info(s"Kafka Stream Was Opened Successfully with ID :: ${streamLine.id}")
    streamLine foreachRDD (rdd => {
      val confFile = config_file
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
            propFile = confFile
//            val auditOut = auditTopic
//            val maxMessageSize = self.maxMessageSize
//            val prodConf = kafkaProducerConf
//            val kafkaOut = KafkaProducerHandler()(prodConf)
//            val auditIO = kafkaOut.writeData(_: String, _: String, auditOut)(maxMessageSize)
            val message = dataItr.next()._2
            val delim = if (message contains "\r\n") "\r\n" else "\n"
            trySplit(message, delim) match {
              case Success(splitted) =>
                val msh = segment(splitted, MSH)
                val pv1 = segment(splitted, PV1)
                val primaryIn1 = segment(splitted, PRIMARY_IN1)
                info(s"MSH: $msh")
                info(s"PV1: $pv1")
                info(s"IN1: $primaryIn1")
                if (eventTypeMatch(msh, adtTypes)) {
                  info(s"Message event type matches")
                  if (singleFieldMatch(msh, facArray, "\\|", 3)) {
                    info("Message facility type matches")
                    if (singleFieldMatch(primaryIn1, insuranceArray, "\\|", 36) &&
                      (stringMatcher(primaryIn1, insuranceNameMatcher, "\\|", 4) ||
                      stringMatcher(primaryIn1, insuranceNameMatcher, "\\|", 9))) {
                      info(s"Patient primary insurance Id is present and name matches")
                      val newPid = removeField(segment(splitted, PID), "\\|", 19)
                      info(s"newPid: $newPid")
                      splitted.update(splitted.indexWhere(segment => segment.startsWith(PID)), newPid)
                      val cleanMessage = splitted.mkString("\n")
                      info(s"Old message: $message")
                      info(s"Message to send: $cleanMessage")
                      if (mqQueue.isDefined) {
                        try{
                          MQAcker(app, app, mqQueue.get)(lookUpProp("mq.hosts"), lookUpProp("mq.manager"), lookUpProp("mq.channel"), numberOfIns = 2)
                          MQAcker.ackMessage(cleanMessage)
                          // tryAndLogThr(auditIO(jsonAudits(msgType)(out._3), header(hl7Str, auditHeader, Left(out._3))), s"ADT-PSG-ACO", error(_: Throwable))
                        } catch {
                          case e: Exception => error(e)
                        }
                      }
                    }
                  }
                }
              case Failure(t) => error(s"Failed to split message: $t")
            }
          }
        })
      }
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
      shutDown()
    }
  }

  private def shutDown(): Unit = {
    sparkUtil shutdownEverything sparkStrCtx
    closeResource(fileSystem)
  }

}