package com.hca.cdm.kafka.producer

import com.hca.cdm.exception.CDMKafkaException
import com.hca.cdm.io.IOConstants._
import com.hca.cdm.log.Logg
import com.hca.cdm.utils.RetryHandler
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}

/**
  * Created by Devaraj Jonnadula on 8/18/2016.
  *
  * Call back Impl holding Ref to data and will retry to Send as per Config
  */
class DataBackedCallBack(val data: ProducerRecord[Array[Byte], Array[Byte]], val kafkaProducer: KafkaProducer[Array[Byte], Array[Byte]], val shouldRetry: Boolean) extends Logg with Callback {

  private var retryHandler: RetryHandler = _

  override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
    if (metadata == null) {
      if (!shouldRetry) {
        error("Failed to Send Record : " + data.value() + " to topic " + data.topic() + " Trying to Sending again .. ", exception)
        kafkaProducer.send(data).get()
      } else {
        this.retryHandler = RetryHandler()
        retrySend(exception)
      }
    } else debug("Record Has been sent to Topic : " + metadata.topic() + " at Offset " + metadata.offset())

  }


  private def retrySend(exception: Exception): Unit = {
    while (retryHandler.tryAgain()) {
      error("Failed to Send Record : " + data.value() + " for topic " + data.topic() + " Trying to send again with Retry Policy .. ", exception)
      if (kafkaProducer.send(data).get() != null) return
    }
    giveUp()
  }


  private def giveUp(): Unit = {
    fatal("Failed to Send Record : " + data.value() + " for topic " + data.topic() + " After tries" + defaultRetries + " Giving UP ")
    throw new CDMKafkaException("Sending Data to Kafka Failed ::  " + data.value() + " with Header :: " + data.key() +
      " to Topic :: " + data.topic())
  }
}
