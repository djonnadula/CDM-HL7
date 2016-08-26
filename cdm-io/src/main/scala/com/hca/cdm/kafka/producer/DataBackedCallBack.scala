package com.hca.cdm.kafka.producer


import com.hca.cdm.io.IOConstants._
import com.hca.cdm.log.Logg
import com.hca.cdm.utils.RetryHandler
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}

/**
  * Created by Devaraj Jonnadula on 8/18/2016.
  */
class DataBackedCallBack(val data: ProducerRecord[Array[Byte], Array[Byte]], val kafkaProducer: KafkaProducer[Array[Byte], Array[Byte]], val shouldRetry: Boolean) extends Logg with Callback {

  private val defaultRetries: Int = defaultRetries
  private val waitBetweenTries = defaultWaitBetweenRetriesMS

  override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
    if (metadata == null) {
      if (!shouldRetry) {
        error("Failed to Send Record : " + data.value() + " to topic " + data.topic() + " Trying to Sending again .. ", exception)
        kafkaProducer.send(data).get()
      } else {
        retrySend(exception,RetryHandler(defaultRetries, waitBetweenTries))
      }
    }
  }


  private def retrySend(exception: Exception, retryHandler: RetryHandler): Unit = {
    while (retryHandler.tryAgain()) {
      error("Failed to Send Record : " + data.value() + " for topic " + data.topic() + " Trying to send again .. ", exception)
      if (kafkaProducer.send(data).get() != null) return
    }
    giveUp()
  }


  private def giveUp(): Unit = {
    fatal("Failed to Send Record : " + data.value() + " for topic " + data.topic() + " After tries" + defaultRetries + " Giving UP ")
  }
}
