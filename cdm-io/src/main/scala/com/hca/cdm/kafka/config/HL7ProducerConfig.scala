package com.hca.cdm.kafka.config

import java.util.{Properties => prop}

import com.hca.cdm.io.IOConstants._
import org.apache.kafka.clients.producer.ProducerConfig._

/**
  * Created by Devaraj Jonnadula on 8/18/2016.
  */
object HL7ProducerConfig {


  def createConfig(topicToProduce: String = ""): prop = {
    val prop = new prop
    prop.put(CLIENT_ID_CONFIG, topicToProduce + "-" + "-" + defaultClientId)
    prop.put(BUFFER_MEMORY_CONFIG, producerBufferMemory)
    prop.put(COMPRESSION_TYPE_CONFIG, defaultCompression)
    prop.put(LINGER_MS_CONFIG, defaultProducerLinger)
   // prop.put(BLOCK_ON_BUFFER_FULL_CONFIG,"true")
    prop.put(MAX_BLOCK_MS_CONFIG, blockOnbufferFull)
    prop.put(REQUEST_TIMEOUT_MS_CONFIG, defaultRequestTimeOut)
    prop.put(ACKS_CONFIG, producerAck)
    prop.put(BOOTSTRAP_SERVERS_CONFIG, kafkaHosts)
    prop.put(BATCH_SIZE_CONFIG, producerBatch)
    prop.put(KEY_SERIALIZER_CLASS_CONFIG, defaultSerializer)
    prop.put(VALUE_SERIALIZER_CLASS_CONFIG, defaultSerializer)
    prop.put(RETRIES_CONFIG, defaultProducerRetries)
    prop.put(SEND_BUFFER_CONFIG, sendBuffer)
    prop.put(RECEIVE_BUFFER_CONFIG, receiveBuffer)
    prop.put(PARTITIONER_CLASS_CONFIG, defaultPartitioner)
    prop
  }

}
