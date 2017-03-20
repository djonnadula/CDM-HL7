package com.hca.cdm.kafka.producer

import java.util
import java.util.Properties
import java.util.concurrent.TimeUnit
import com.hca.cdm.io.DataWriter
import com.hca.cdm.io.IOConstants._
import com.hca.cdm.log.Logg
import kafka.utils.SystemTime
import com.hca.cdm._
import com.hca.cdm.exception.CDMKafkaException
import com.hca.cdm.hadoop.OverSizeHandler
import com.hca.cdm.kafka.config.HL7ProducerConfig.{createConfig => conf}
import com.hca.cdm.kafka.util.{TopicUtil => topicUtil}
import com.hca.cdm.utils.RetryHandler
import org.apache.kafka.clients.producer.ProducerConfig._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.PartitionInfo
import scala.collection.concurrent.TrieMap

/**
  * Created by Devaraj Jonnadula on 8/18/2016.
  *
  * Handler For Producing Data to Kafka
  */
class KafkaProducerHandler private(private val topicToProduce: String = "", private val publishToMultiTopic: Boolean = false)(implicit val props: Properties)
  extends Logg with AutoCloseable with DataWriter {

  private val check: (Boolean, String) = validate(topicToProduce, publishToMultiTopic)
  require(check._1, check._2)
  private var id: String = ""
  private var producer: KafkaProducer[Array[Byte], Array[Byte]] = _
  private var producerStarted: Boolean = false
  private var partitionForTopic: util.List[PartitionInfo] = _
  private var lastFlush = 0L
  private var sHook: Thread = _
  private val topicsToProduce = new TrieMap[String, Boolean]()
  private val messagesProduced = new TrieMap[String, Long]()
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

  def writeData(data: AnyRef, header: AnyRef, topic: String = topicToProduce)(sizeThreshold: Int = 4194304,
                                                                              overSizeHandler: OverSizeHandler = null): Unit = {
    /* topicsToProduce get topic match {
      case Some(t) => handleData(data, header, topic)
      case _ => topicsToProduce += topic -> topicUtil.createTopicIfNotExist(topic)
        handleData(data, header, topic)
    } */
    handleData(data, header, topic, sizeThreshold, overSizeHandler)
  }

  def getTotalWritten(topic: String): Long = {
    messagesProduced get topic match {
      case Some(l) => l
      case _ => -1
    }
  }

  def getTotalWritten: Map[String, Long] = messagesProduced.toMap


  private def handleData(data: AnyRef, header: Any, topic: String, sizeThreshold: Int, overSizeHandler: OverSizeHandler): Unit = {
    if (valid(data) & valid(topic) & !topic.trim.isEmpty) {
      if (!IOCanHandle(data, sizeThreshold) & overSizeHandler != null) {
        info("Record cannot be deal with this handler :: " + getClass + "  So Delivering to OversizeHandler :: " + overSizeHandler)
        overSizeHandler handle data
        return
      }
      valid(header) match {
        case true => (header, data) match {
          case (k: String, v: String) => produceData(new ProducerRecord[Array[Byte], Array[Byte]](topic, k.getBytes(UTF8), v.getBytes(UTF8)))
          case (k: Array[Byte], v: Array[Byte]) => produceData(new ProducerRecord[Array[Byte], Array[Byte]](topic, k, v))
          case (k: Array[Byte], v: String) => produceData(new ProducerRecord[Array[Byte], Array[Byte]](topic, k, v.getBytes(UTF8)))
          case (k: String, v: Array[Byte]) => produceData(new ProducerRecord[Array[Byte], Array[Byte]](topic, k.getBytes(UTF8), v))
          case _ => throw new UnsupportedOperationException("This Type of Operation not supported for this Type " + data + " with Header :: " + header)
        }
        case _ => data match {
          case v: String => produceData(new ProducerRecord[Array[Byte], Array[Byte]](topic, v.getBytes(UTF8)))
          case v: Array[Byte] => produceData(new ProducerRecord[Array[Byte], Array[Byte]](topic, v))
          case _ => throw new UnsupportedOperationException("This Type of Operation not supported for this Type " + data + " with Header :: " + header)

        }

      }
    }
    else throw new CDMKafkaException("Cannot Send Invalid Data to Kafka ::  " + data + " with Header :: " + header + " to Topic :: " + topic)
  }


  private def produceData(record: ProducerRecord[Array[Byte], Array[Byte]]): Unit = {
    if (this.producer != null && this.producerStarted) {
      this.producer.send(record, new DataBackedCallBack(record, this.producer, true))
      messagesProduced update(record.topic(), inc(messagesProduced getOrElse(record.topic(), 0L)))
    }
  }

  private def flushEverything(producer: KafkaProducer[Array[Byte], Array[Byte]]) = {
    try producer flush()
    catch {
      case ie: InterruptedException => error("Flushing failed ", ie)
    }
  }

  def reset(): Unit = messagesProduced transform ((k, v) => 0L)

  private def updateFlushTime() = this.lastFlush = SystemTime.milliseconds

  @throws(classOf[CDMKafkaException])
  private def initialise(): Unit = {
    if (publishToMultiTopic match {
      case true => true
      case _ => if (!topicToProduce.trim.isEmpty) {
        topicUtil.createTopicIfNotExist(topicToProduce)
        topicsToProduce += topicToProduce -> true
      }
        true
    }) {
      handleProducer()
      if (this.producer != null && this.producerStarted) {
        sHook = newThread(defaultClientId + " SHook", runnable({
          flushEverything(producer)
          producer.close(1, TimeUnit.HOURS)
          this.producer.close()
        }))
        registerHook(sHook)
        info(s"Registered SHook for $producer with Name ${sHook.getName}")
      }
    } else {
      fatal("Topic Doesn't Exist for Producing  and creating one with default config failed for topic :: " + topicToProduce)
      throw new CDMKafkaException("Topic Doesn't Exist for Producing and creating failed for topic :: " + topicToProduce)
    }
  }


  private def handleProducer(): Unit = {
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
      this.producer = null
    }
  }


}

object KafkaProducerHandler extends AutoCloseable {

  private var producer: KafkaProducerHandler = _
  private val lock = new Object()

  def apply(implicit props: Properties): KafkaProducerHandler = {
    def createIfNotExist = new (() => KafkaProducerHandler) {
      override def apply(): KafkaProducerHandler = new KafkaProducerHandler(EMPTYSTR, true)(props)
    }
    createProducer(createIfNotExist)
  }

  def apply(topicToProduce: String)(implicit props: Properties): KafkaProducerHandler = {
    def createIfNotExist = new (() => KafkaProducerHandler) {
      override def apply(): KafkaProducerHandler = new KafkaProducerHandler(topicToProduce, false)(props)
    }
    createProducer(createIfNotExist)
  }

  def apply(multiDest: Boolean = true)(implicit props: Properties): KafkaProducerHandler = {
    def createIfNotExist = new (() => KafkaProducerHandler) {
      override def apply(): KafkaProducerHandler = new KafkaProducerHandler("", true)(props)
    }
    createProducer(createIfNotExist)
  }

  private def createProducer(createIfNotExist: () => KafkaProducerHandler): KafkaProducerHandler = {
    lock.synchronized(
      producer == null match {
        case true =>
          producer = createIfNotExist()
          info(s"Created Kafka Producer handler $producer")
          producer
        case _ => producer
      })
  }

  override def close(): Unit = {
    lock.synchronized({
      info(s"Closing Kafka Producer handler $producer")
      closeResource(producer)
    })
  }
}
