/*
package com.hca.cdm.mq

import java.util.Properties
import com.hca.cdm.log.Logg
import javax.jms._
import com.hca.cdm._
import com.hca.cdm.io.IOConstants._
import com.ibm.mq.jms.{MQQueue, _}
import com.ibm.msg.client.wmq.compat.jms.internal.JMSC


/**
  * Created by Devaraj Jonnadula on 9/28/2016.
  */
class MQWriterHandler(implicit val props: Properties) extends Logg with AutoCloseable {

  require(props != null, "Properties Cannot be null ")
  private var connection: MQQueueConnection = _
  private var session: MQQueueSession = _
  private var writer: MQQueueSender = _
  private var queue: MQQueue = _
  private var sHook: Thread = _
  private val MESSAGE_TIMEOUT = 1000L
  private var totalWritten = 0L
  initialise()

  def writeData(data: String): Unit = {
    if (tryAndLogErrorMes(writer.send(queue, session.createTextMessage(data)), error(_: String))) totalWritten = inc(totalWritten)
  }

  private def initialise(): Unit = {
    val factory = new MQQueueConnectionFactory()
    factory.setConnectionNameList(lookUpProp("mq.hosts"))
    factory.setPort(lookUpProp("mq.port") toInt)
    factory.setTransportType(JMSC.MQJMS_TP_CLIENT_MQ_TCPIP)
    factory.setQueueManager(lookUpProp("mq.manager"))
    factory.setClientID(defaultClientId)
    factory.setChannel(lookUpProp("mq.channel"))
    factory.setAppName(lookUpProp("hl7.app"))
    connection = factory.createQueueConnection().asInstanceOf[MQQueueConnection]
    session = connection.createQueueSession(false, Session.CLIENT_ACKNOWLEDGE).asInstanceOf[MQQueueSession]
    queue = session.createQueue(lookUpProp("mq.queue")).asInstanceOf[MQQueue]
    writer = session.createSender(queue).asInstanceOf[MQQueueSender]
    sHook = newThread(defaultClientId + " MQ SHook", runnable({
      close()
    }))
    connection start()

  }

  override def close(): Unit = {
    if (session != null) session close()
    if (connection != null) connection close()
  }
}
*/
