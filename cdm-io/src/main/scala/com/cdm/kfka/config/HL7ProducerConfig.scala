package com.cdm.kfka.config

import java.util.{Properties => prop}
import com.cdm._
import com.cdm.exception.CDMKafkaException
import com.cdm.io.IOConstants._
import org.apache.kafka.clients.producer.ProducerConfig._
import org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG

/**
  * Created by Devaraj Jonnadula on 8/18/2016.
  *
  * Config for Kafka Producer
  */
object HL7ProducerConfig {


  def createConfig(topicToProduce: String = EMPTYSTR): prop = {
    val prop = new prop
    prop.put(BUFFER_MEMORY_CONFIG, producerBufferMemory)
    prop.put(COMPRESSION_TYPE_CONFIG, defaultCompression)
    prop.put(LINGER_MS_CONFIG, defaultProducerLinger)
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
    prop.put(MAX_REQUEST_SIZE_CONFIG, requestMaxSize)
    if (tryAndReturnDefaultValue(asFunc(lookUpProp("kafka.security.protocol")), EMPTYSTR) != EMPTYSTR) {
      lookUpProp("kafka.security.protocol") match {
        case "SSL" =>
          prop.put(SECURITY_PROTOCOL_CONFIG, lookUpProp("kafka.security.protocol"))
          prop.put("ssl.truststore.location", lookUpProp("ssl.truststore.location"))
          prop.put("ssl.truststore.password", lookUpProp("ssl.truststore.password"))
        case _ => throw new CDMKafkaException("Not yet available kafka.security.protocol")
      }
    }
    tryAndReturnDefaultValue0(lookUpProp("hl7.kafka.prod.cfg"), EMPTYSTR) match {
      case `EMPTYSTR` =>
      case cfg: String =>
        cfg.split(";", -1).foreach { conf =>
          if (valid(conf)) {
            val actConf = conf.split(":")
            if (valid(actConf, 2)) prop.put(actConf(0), actConf(1))
          }
        }
    }
    prop
  }
}
