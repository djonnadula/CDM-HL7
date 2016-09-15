package com.hca.cdm.kafka.producer

import java.util
import java.util.concurrent.TimeUnit

import com.hca.cdm.io.DataWriter
import com.hca.cdm.io.IOConstants._
import com.hca.cdm.log.Logg
import kafka.utils.SystemTime
import com.hca.cdm._
import com.hca.cdm.exception.CDMKafkaException
import com.hca.cdm.kafka.config.HL7ProducerConfig.{createConfig => conf}
import com.hca.cdm.kafka.util.{TopicUtil => topicUtil}
import com.hca.cdm.utils.RetryHandler
import org.apache.kafka.clients.producer.ProducerConfig._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.PartitionInfo

import scala.collection.concurrent.TrieMap

/**
  * Created by Devaraj Jonnadula on 8/18/2016.
  */
class KafkaProducerHandler private(private val topicToProduce: String = "", private val publishToMultiTopic: Boolean = false)
  extends Logg with AutoCloseable with DataWriter {

  private val check: (Boolean, String) = validate(topicToProduce, publishToMultiTopic)
  require(check._1, check._2)
  private var id: String = ""
  private var producer: KafkaProducer[Array[Byte], Array[Byte]] = _
  private var producerStarted: Boolean = false
  private var partitionForTopic: util.List[PartitionInfo] = _
  private var lastFlush = 0L
  private var sHook: Thread = _
  private var topicsToProduce = new TrieMap[String, Boolean]()
  private var messagesProduced = new TrieMap[String, Long]()
  initialise()

  private def validate(topicToProduce: String, publishToMultiTopic: Boolean): (Boolean, String) = {
    val validReq = publishToMultiTopic match {
      case true => true
      case _ => topicToProduce != null && !topicToProduce.trim.isEmpty
    }
    if (validReq) {
      info("Producer Initialisation Started To produce data to Topic :: " + topicToProduce)
      (validReq, "Producer Initialisation Valid")
    }
    else {
      error("Invalid Arguments topicToProduce not Valid")
      (validReq, "InValid Initialisation")
    }
  }

  def writeData(data: Any, header: Any, topic: String = topicToProduce): Unit = {
    topicsToProduce.get(topic) match {
      case Some(t) => handleData(data, header, topic)
      case _ => topicsToProduce += topic -> topicUtil.createTopicIfNotExist(topic)
        handleData(data, header, topic)
    }

  }

  def getTotalWritten(topic: String): Long = {
    messagesProduced get topic match {
      case Some(l) => l
      case _ => -1
    }
  }

  def getTotalWritten: Map[String, Long] = messagesProduced.toMap


  private def handleData(data: Any, header: Any, topic: String): Unit = {
    if (valid(data) & valid(topic)) {
      valid(header) match {
        case true => (data, header) match {
          case (k: String, v: String) => produceData(new ProducerRecord[Array[Byte], Array[Byte]](topic, k.getBytes(UTF8), v.getBytes(UTF8)))
          case (k: Array[Byte], v: Array[Byte]) => produceData(new ProducerRecord[Array[Byte], Array[Byte]](topic, k, v))
          case (k: Array[Byte], v: String) => produceData(new ProducerRecord[Array[Byte], Array[Byte]](topic, k, v.getBytes(UTF8)))
          case (k: String, v: Array[Byte]) => produceData(new ProducerRecord[Array[Byte], Array[Byte]](topic, k.getBytes(UTF8), v))
        }
        case _ => data match {
          case v: String => produceData(new ProducerRecord[Array[Byte], Array[Byte]](topic, v.getBytes(UTF8)))
          case v: Array[Byte] => produceData(new ProducerRecord[Array[Byte], Array[Byte]](topic, v))

        }

      }
    }
    else fatal("Cannot Send Invalid Data to Kafka ::  " + data)
  }


  private def produceData(record: ProducerRecord[Array[Byte], Array[Byte]]): Unit = {
    if (this.producer != null && this.producerStarted) {
      this.producer.send(record, new DataBackedCallBack(record, this.producer, true))
      messagesProduced update(record.topic(), inc(messagesProduced getOrElse(record.topic(), 0L)))
    }
  }

  private def flushEverything(producer: KafkaProducer[Array[Byte], Array[Byte]]) = {
    try producer.flush()
    catch {
      case ie: InterruptedException => error("Flushing failed ", ie)
    }
  }

  def reset(): Unit = messagesProduced.transform((k, v) => 0L)


  private def updateFlushTime() = this.lastFlush = SystemTime.milliseconds


  @throws(classOf[CDMKafkaException])
  private def initialise(): Unit = {
    if (!publishToMultiTopic & topicUtil.createTopicIfNotExist(topicToProduce)) {
      topicsToProduce += topicToProduce -> true
      handleProducer()
      if (this.producer != null && this.producerStarted) {
        findPartitionsForTopic
        sHook = newThread(defaultClientId + " SHook", runnable({
          flushEverything(producer)
          producer.close(1, TimeUnit.HOURS)
          this.producer.close()
        }))
        registerHook(sHook)
      }
    } else {
      fatal("Topic Doesn't Exist for Producing  and creating one with default config failed for topic :: " + topicToProduce)
      throw new CDMKafkaException("Topic Doesn't Exist for Producing and creating failed for topic :: " + topicToProduce)
    }
  }


  private def handleProducer(): Unit = {
    val props = conf(topicToProduce)
    id = props.getProperty(CLIENT_ID_CONFIG, defaultClientId)
    try {
      info("Starting Kafka Producer")
      this.producer = new KafkaProducer(props)
      this.producerStarted = true
    } catch {
      case ex: Exception => error("Producer Connection Failed. Will Try To make connection based on Number of Re Tries Assigned", ex)
        var tryCount: Int = 1
        val retry = RetryHandler(defaultRetries, defaultWaitBetweenRetriesMS)
        while (retry.tryAgain()) {
          try {
            this.producer = new KafkaProducer(props)
            this.producerStarted = true
            info("Producer Started After retries " + tryCount + " for Topic :: " + topicToProduce)
            return
          } catch {
            case e: Exception => error("Producer Connection Failed For Try :: " + tryCount, e)
              tryCount += 1
              if (tryCount == defaultRetries) {
                throw new CDMKafkaException("Cannot Start Producer After Retries " + tryCount + " For Topic " + topicToProduce, e)
              }
          }
        }
    }
  }

  private def findPartitionsForTopic: util.List[PartitionInfo] = {
    try {
      this.partitionForTopic = producer.partitionsFor(topicToProduce)
      info("Partitions for Topic :: " + this.partitionForTopic.toString)
    } catch {
      case ie: InterruptedException => error("Unable to Check partition Info for Topic :: " + topicToProduce, ie)
    }
    this.partitionForTopic
  }


  override def close(): Unit = {
    if (this.producer != null) {
      flushEverything(producer)
      this.producer.close(1, TimeUnit.HOURS)
      this.producer.close()
      unregister(sHook)
    }
  }


}

object KafkaProducerHandler {
  def apply(topicToProduce: String): KafkaProducerHandler = new KafkaProducerHandler(topicToProduce, false)

  def apply(multiDest: Boolean): KafkaProducerHandler = new KafkaProducerHandler("", true)
}
