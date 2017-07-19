package com.hca.cdm.job.psg.aco

import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}
import java.lang.System.{getenv => fromEnv}
import java.util
import java.util.Date

import com.hca.cdm._
import com.hca.cdm.kafka.config.HL7ConsumerConfig.{createConfig => consumerConf}
import com.hca.cdm.log.Logg
import com.hca.cdm.spark.{Hl7SparkUtil => sparkUtil}
import com.hca.cdm.hl7.constants.HL7Constants._
import com.hca.cdm.mq.publisher.MQAcker
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.apache.spark.streaming.StreamingContext

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
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
  private lazy val fileSystem = FileSystem.get(new Configuration())
  private val appHomeDir = fileSystem.getHomeDirectory.toString
  private val stagingDir = fromEnv("SPARK_YARN_STAGING_DIR")
  private lazy val adtTypes = lookUpProp("PSGACOADT.adt.types").split(",")
  private lazy val insuranceFileLocation = new Path(lookUpProp("PSGACOADT.insurance.file.location"))
  private lazy val facFileLocation = new Path(lookUpProp("PSGACOADT.fac.file.location"))
  private lazy val insuranceArray = readFile(insuranceFileLocation)
  private lazy val facArray = readFile(facFileLocation)
  private lazy val outputFile = new Path(lookUpProp("PSGACOADT.output.file.location"))
  private lazy val mqQueue = enabled(lookUpProp("mq.queueResponse"))

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

  // TODO: Remove social information
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
        val wsmqQueue = self.mqQueue
        val appName = self.app
        info(s"Got RDD ${rdd.id} with Partitions :: " + rdd.partitions.length + " and Messages Cnt:: " + messagesInRDD + " Executing Asynchronously Each of Them.")
        rdd foreachPartitionAsync (dataItr => {
          if (dataItr.nonEmpty) {
            val message = dataItr.next()._2
            val delim = if (message contains "\r\n") "\r\n" else "\n"
            val insuranceIds = new ArrayBuffer[String]
            Try(message.split(delim)) match {
              case Success(splitted) =>
                val matchMessageEventType: Boolean = segment(splitted, MSH) match {
                  case Some(segment) =>
                    splitAndReturn(segment, "\\|", 8) match {
                      case Success(messageType) =>
                        splitAndReturn(messageType, "\\^", 1) match {
                          case Success(eventType) =>
                            info(s"Found an event type: $eventType")
                            adtTypes.contains(eventType)
                          case Failure(t) =>
                            warn("MSH Segment does not contain an event type")
                            false
                        }
                      case Failure(t) =>
                        error("MSH Segment does not contain a message type")
                        false
                    }
                  case None =>
                    error("Message does not contain MSH segment")
                    false
                }
                if (matchMessageEventType) {
                  info(s"Message event type matches")
                  val matchFacility: Boolean = segment(splitted, PV1) match {
                    case Some(segment) =>
                      splitAndReturn(segment, "\\|", 39) match {
                        case Success(facility) =>
                          info(s"Found a facility: $facility")
                          facArray.contains(facility)
                        case Failure(t) =>
                          warn("PV1 Segment does not contain correct facility")
                          false
                      }
                    case None =>
                      warn("PV1 Segment does not contain PV1 segment")
                      false
                  }
                  if (matchFacility) {
                    info("Message facility type matches")
                    splitted.foreach(segment => {
                      if (segment.startsWith(IN1)) {
                        splitAndReturn(segment, "\\|", 36) match {
                          case Success(res) =>
                            info(s"Found policy_num: $res")
                            insuranceIds += res
                          case Failure(t) => warn(s"No policy_num for segment")
                        }
                      }
                    })
                    if (insuranceIds.nonEmpty) {
                      insuranceIds.foreach(id => {
                        if (id.nonEmpty && insuranceArray.contains(id)) {
                          info(s"Found HICN: $id")
                          info(s"Message to send: $message")
                          val msg = message.split(delim)
                          segment(splitted, PID) match {
                            case Some(segment) =>
                              splitAndReturn(segment, "\\|", 39) match {
                                case Success(facility) =>
                                  info(s"Found a facility: $facility")
                                  facArray.contains(facility)
                                case Failure(t) =>
                                  warn("PV1 Segment does not contain correct facility")
                                  false
                              }
                            case None =>
                              warn("PV1 Segment does not contain PV1 segment")
                              false
                          }
//                          writeToFile(outputFile, message)
                          if (wsmqQueue.isDefined) {
                            MQAcker(appName, appName, wsmqQueue.get)(lookUpProp("mq.hosts"), lookUpProp("mq.manager"), lookUpProp("mq.channel"), numberOfIns = 2)
                            MQAcker.ackMessage(message)
                          }
                        }
                      })
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

  def splitAndReturn(segment: String, delimiter: String, returnIndex: Int): Try[String] = {
    Try(segment.split(delimiter)(returnIndex))
  }

  def splitAndReplace(segment: String, delimiter: String, replaceIndex: Int, replaceValue: String): Try[Any] = {
    Try(segment.split(delimiter)(replaceIndex).replace(_, replaceValue))
  }

  def segment(message: Array[String], segType: String): Option[String] = {
    message.find(segment => segment.startsWith(segType))
  }

  def readFile(path: Path): Array[AnyRef] = {
    val br = new BufferedReader(new InputStreamReader(fileSystem.open(path)))
    br.lines().toArray
  }

  def writeToFile(path: Path, line: String): Unit = {
    val br = new BufferedWriter(new OutputStreamWriter(fileSystem.append(path)))
    br.write(line)
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

  case class interestedFields(messageType: String, facility: String, insuranceIds: ArrayBuffer[String])

}