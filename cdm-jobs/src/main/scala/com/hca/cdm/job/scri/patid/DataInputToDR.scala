
package com.hca.cdm.job.scri.patid

import com.hca.cdm._
import com.hca.cdm.log.Logg
import com.hca.cdm.kfka.config.HL7ProducerConfig.{createConfig => producerConf}
import com.hca.cdm.kfka.producer.{KafkaProducerHandler => KProducer}

/**
  * Created by Devaraj Jonnadula on 3/26/2018.
  */
object DataInputToDR extends App with Logg {
  self =>

  reload(args(0))
  private val destTopic = lookUpProp("patid.dr.in.topic")
  private val writer = KProducer()(producerConf())
  private var msgCntr = 0L
  registerHook(newThread(s"SHook-${self.getClass.getSimpleName}${lookUpProp("app")}", runnable({
    shutDown()
    info(s"$self shutdown hook completed")
  })))
  readFile(lookUpProp("patid.dr.in")).getLines().foreach { msg =>
    msgCntr = inc(msgCntr)
    writer.writeData(msg, null, destTopic)
  }


  private def shutDown(): Unit = {
    info(s"Total Messages written to $destTopic :: $msgCntr")
    closeResource(writer)
    info(s"$self shutdown completed")
  }

}

