package com.hca.cdm.job.psg.aco

import java.lang.System.{getenv => fromEnv}
import java.net.InetSocketAddress
import java.util.Date

import akka.actor.ActorSystem
import akka.util.ByteString
import com.hca.cdm._
import com.hca.cdm.hl7.constants.HL7Constants._
import com.hca.cdm.job.psg.aco.PsgAcoAdtJobUtils._
import com.hca.cdm.kfka.config.HL7ConsumerConfig.{createConfig => consumerConf}
import com.hca.cdm.kfka.config.HL7ProducerConfig._
import com.hca.cdm.log.Logg
import com.hca.cdm.notification.sendMail
import com.hca.cdm.spark.{Hl7SparkUtil => sparkUtil}
import com.hca.cdm.tcp.ActorSupervisor
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.HasOffsetRanges

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success}


/**
  * PSG ACO ADT Job
  * This job reads the HL7CDMADT Kafka topic and does some filtering on the messages
  * If the message passes all filters, send the message to Cloverleaf via a tcp client
  *
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
  private lazy val insArray = PsgAcoAdtJobUtils.readFile(insuranceFileLocation, fileSystem)
  private lazy val facArray = PsgAcoAdtJobUtils.readFile(facFileLocation, fileSystem)
  private lazy val outputFile = new Path(lookUpProp("PSGACOADT.output.file.location"))
  private lazy val mqQueue = enabled(lookUpProp("mq.queueResponse"))
  private lazy val insNameMatcher = lookUpProp("PSGACOADT.insurance.name.match").split(",")
  private lazy val auditTopic = lookUpProp("hl7.audit")
  private lazy val maxMessageSize = lookUpProp("hl7.message.max") toInt
  private lazy val kafkaProducerConf = createConfig(auditTopic)
  private lazy val cloverleafAddr = lookUpProp("PSGACOADT.cloverleaf.addr")
  private lazy val cloverleafPort = lookUpProp("PSGACOADT.cloverleaf.port").toInt
  private lazy val tcpConnectionWaitTime = lookUpProp("PSGACOADT.tcp.conn.wait.time").toLong
  private lazy val message_begin: Char = 0x0b
  private lazy val segment_end: Char = 0x1c
  private lazy val message_end: Char = 0x0d
  private lazy val actorSystem = ActorSystem.create("PSGActorSystem")
  private lazy val supervisor = actorSystem.actorOf(ActorSupervisor.props(new InetSocketAddress(cloverleafAddr, cloverleafPort)), "supervisor")

  private var sparkStrCtx: StreamingContext = initContext
  printConfig()
  startStreams()

  /**
    * Creates a spark streaming context if it does not exist and runs the job
    * @return returns the streaming context
    */
  private def newCtxIfNotExist = new (() => StreamingContext) {
    override def apply(): StreamingContext = {
      val ctx = sparkUtil createStreamingContext(sparkConf, batchDuration)
      info(s"New Checkpoint Created for $app $ctx")
      runJob(ctx)
      ctx
    }
  }

  /**
    * Checkpointing logic
    * @return spark streaming context
    */
  private def initContext: StreamingContext = {
    sparkStrCtx = if (checkpointEnable) sparkUtil streamingContext(checkPoint, newCtxIfNotExist) else sparkUtil createStreamingContext(sparkConf, batchDuration)
    sparkStrCtx.sparkContext setJobDescription lookUpProp("job.desc")
    if (!checkpointEnable) runJob(sparkStrCtx)
    // createTopicIfNotExist(auditTopic, segmentPartitions = false)
    sparkStrCtx
  }

  /**
    * Main business logic
    * @param sparkStrCtx spark streaming context to run the business logic in
    */
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
        info(s"Got RDD ${rdd.id} with Partitions :: " + rdd.partitions.length + " and Messages Cnt:: " + messagesInRDD +
          " Executing Asynchronously Each of Them.")
        rdd foreachPartitionAsync  (dataItr => {
          if (dataItr.nonEmpty) {
            propFile = confFile
            val actorSys = actorSystem
            val msg1 = new StringBuilder()
            val actorSupervisor = supervisor
            val insuranceArray = insArray
            val insuranceNameMatcher = insNameMatcher
            val faciltyArray = facArray
            val adtArray = adtTypes
            val begin_of_message = message_begin
            val end_of_segment = segment_end
            val end_of_message = message_end
            val tcpConWaitTime = tcpConnectionWaitTime
            val appName = app
            val message = dataItr.next()._2
            val delim = if (message contains "\r\n") "\r\n" else "\n"
            trySplit(message, delim) match {
              case Success(splitted) =>

                val msh = segment(splitted, MSH)
                val pv1 = segment(splitted, PV1)
                val primaryIn1 = segment(splitted, PRIMARY_IN1)
                msh.foreach(seg => debug("MSH: " + seg))
                pv1.foreach(seg => debug("PV1: " + seg))
                primaryIn1.foreach(seg => debug("IN1: " + seg))

                // Check that the message event type is in this list
                // A01,A02,A03,A04,A05,A06,A07,A08,A09,A10,A11,A12,A13,A18
                if (eventTypeMatch(msh, adtArray)) {
                  info(s"Message event type matches")

                  // Check that the facility is in this list (Fac_Mnemonic_Roster.txt)
                  // COCBR,COCDT,COCEH,COCFH,COCGP,COCLR,COCLW,COCMMC,COCNP,COCNS,COCOH,COCPOP,COCSE,COCTAC,COCBP,COCSMC
                  if (singleFieldMatch(msh, faciltyArray, "\\|", 3)) {
                    info("Message facility type matches")

                    // Check that the insurance name matches 'Medicare' and that the Medicare Insurance Number is in the Patient_Roster_PSG_Integral.txt
                    if (singleFieldMatch(primaryIn1, insuranceArray, "\\|", 36) &&
                      (stringMatcher(primaryIn1, insuranceNameMatcher, "\\|", 4) ||
                      stringMatcher(primaryIn1, insuranceNameMatcher, "\\|", 9))) {
                      info(s"Patient primary insurance Id is present and name matches")

                      // Remove SSNS from PID and GT1 segments
                      // PID SSN removal
                      val pidSegment = segment(splitted, PID)
                      val ssn = getField(pidSegment, "\\|", 19)
                      val newPid = removeField(pidSegment, "\\|", 19)
                      debug(s"newPid: $newPid")
                      splitted.update(splitted.indexWhere(segment => segment.startsWith(PID)), newPid)

                      // GT1 SSN removal
                      val newGT1s = new ArrayBuffer[String]
                      val gt1Segment = segment(splitted, GT1)
                      gt1Segment.foreach(seg => {
                        val splitSeg = seg.split("\\|")
                        if (splitSeg(12).nonEmpty) {
                          splitSeg.update(12, "")
                          newGT1s += splitSeg.mkString("|")
                        } else {
                          info(s"Segment contains no value at index: 12")
                          newGT1s += seg
                        }
                      })

                      // Update all GT1s (there could be more than 1 GT1 segment)
                      for (gt1 <- newGT1s) {
                        try {
                          splitted.update(splitted.indexWhere(segment => {
                            val index = GT1 + "|" + (newGT1s.indexOf(gt1) + 1)
                            val ind = segment.startsWith(index)
                            ind
                          }), gt1)
                        } catch {
                          case e: Exception => error(e)
                        }
                      }

                      // Connect all segments by \r
                      val cleanMessage = splitted.mkString("\r")

                      // Search entire message for any other SSNs and replace them with an empty string
                      val finalMessage = cleanMessage.replace(ssn, "")
                      debug(s"Old message: $message")
                      debug(s"Message to send: $finalMessage")
                      try{
                        // Send an email saying that a message has been found and is being sent
                        sendMail(appName, "Found message to send", notification.TaskState.NORMAL, statsReport = false,
                          lookUpProp("email.to").split(COMMA))
                        val msg1 = new StringBuilder()
                        // Add the MLLP Procotol message headers/footers (http://camel.apache.org/hl7.html)
                        msg1.append(begin_of_message).append(finalMessage).append(end_of_segment).append(end_of_message)
                        Thread.sleep(tcpConWaitTime)
                        // Send the message to the Akka tcp message framework
                        actorSupervisor ! ByteString(msg1.toString())
                      } catch {
                        case e: Exception => error(e)
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

  /**
    * Start the spark streaming context
    */
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

  /**
    * Shutdown the job
    */
  private def shutDown(): Unit = {
    sparkUtil shutdownEverything sparkStrCtx
    closeResource(fileSystem)
  }

}