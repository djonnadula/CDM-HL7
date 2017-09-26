package com.hca.cdm.job

import java.util.ArrayList

import com.hca.cdm._
import com.hca.cdm.kfka.config.{HL7ConsumerConfig, HL7ProducerConfig}
import com.hca.cdm.kfka.producer.KafkaProducerHandler
import com.hca.cdm.log.Logg
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.log4j.PropertyConfigurator.configure

import scala.collection.JavaConverters._


/**
  * Created by Devaraj Jonnadula on 8/9/2017.
  */
object SaslKafkaTest extends Logg with App{
  configure(currThread.getContextClassLoader.getResource("cdm-log4j.properties"))
  reload(args(0))
  val writeToTopic = lookUpProp("topic.to.write")
  val config = HL7ConsumerConfig.createConfig("dummy")
  val consumer = new KafkaConsumer[String,String](config)
  info("Topics from Brokers :: ")
  consumer.listTopics().asScala.foreach({
    x =>  info(x._1)
      x._2.asScala.foreach(y =>info(y+ EMPTYSTR))
  })

  val kafkaProducerConf = HL7ProducerConfig.createConfig(writeToTopic)
  val producer =  KafkaProducerHandler(kafkaProducerConf)
  val numberofMsg = lookUpProp("number.messages.to.publish").toInt
  for(msg <- 0 until numberofMsg ){
    producer.writeData(s"test$msg",s"test$msg", writeToTopic)(4194304,null)

  }
  producer.close()
  val consumeFrom = new ArrayList[String]()
  consumeFrom.add(writeToTopic)
  consumer.subscribe(consumeFrom)
  var toConsume = numberofMsg
  while(toConsume >0){
    val data = consumer.poll(2000)
    data.asScala.foreach(
      x =>{info(s"Message From Topic ${x.value()} partition ${x.partition()}")
        toConsume -= 1
      })
  }
  consumer.close()

}
