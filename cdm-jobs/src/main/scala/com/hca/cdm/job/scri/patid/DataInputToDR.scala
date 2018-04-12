
package com.hca.cdm.job.scri.patid

import com.hca.cdm._
import com.hca.cdm.kfka.util.TopicUtil.{createTopicIfNotExist => createTopic}
import com.hca.cdm.log.Logg
import com.hca.cdm.kfka.config.HL7ProducerConfig.{createConfig => producerConf}
import com.hca.cdm.kfka.producer.{KafkaProducerHandler => KProducer}
import org.apache.log4j.PropertyConfigurator.configure

/**
  * Created by Devaraj Jonnadula on 3/26/2018.
  */
object DataInputToDR extends App with Logg {
  self =>
  configure(currThread.getContextClassLoader.getResource("cdm-log4j.properties"))
  reload(args(0))
  private val destTopic = lookUpProp("patid.dr.in.topic")
  private val writer = KProducer()(producerConf())
  private var msgCntr = 0L
  createTopic(destTopic, segmentPartitions = false)
  registerHook(newThread(s"SHook-${self.getClass.getSimpleName}${lookUpProp("app")}", runnable({
    shutDown()
    info(s"$self shutdown hook completed")
  })))
  readFile(lookUpProp("patid.dr.in")).getLines().foreach { msg =>
    msgCntr = inc(msgCntr)
    writer.writeData(msg, null, destTopic)(4194304,null)
  }


  private def shutDown(): Unit = {
    info(s"Total Messages written to $destTopic :: $msgCntr")
    info(s"$self shutdown completed")
  }

}

