package com.cdm.mq

import javax.jms.{Message, TextMessage, _}

import com.cdm.log.Logg
import com.ibm.mq.jms.{MQQueueReceiver, _}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import scala.util.{Success, Try}

/**
  * Created by dof7475 on 7/25/2017.
  */
class WsmqConsumer(appName: String, host: String, port: Int, qm: String, channel: String, qn: String)
  extends Receiver[String](storageLevel = StorageLevel.MEMORY_ONLY_SER) with AutoCloseable with Logg {

  private val createFactory = {
    val temp = new MQQueueConnectionFactory
    temp.setConnectionNameList(host)
    temp.setPort(port)
    temp.setTransportType(JMSC.MQJMS_TP_CLIENT_MQ_TCPIP)
    temp.setQueueManager(qm)
    temp.setChannel(channel)
    temp.setMsgBatchSize(200)
    info("Connection Factory Created to Hosts  :: " + temp.getConnectionNameList)
    temp
  }
  private var connection: MQQueueConnection = _
  private var session: MQQueueSession = _
  private var queue: MQQueue = _
  private var receiver: MQQueueReceiver = _
  private var consumer: MessageConsumer = _

  private case class ExceptionReporter() extends ExceptionListener {
    override def onException(e: JMSException): Unit = {
      restart("Connection Broken Trying to reestablish", e, 10000)
    }
  }

  override def onStart(): Unit = {
    makeConnection()
  }

  private def makeConnection() {
    try {
      connection = createFactory.createQueueConnection.asInstanceOf[MQQueueConnection]
      connection.setExceptionListener(new ExceptionReporter)
      session = connection.createQueueSession(false, Session.CLIENT_ACKNOWLEDGE).asInstanceOf[MQQueueSession]
      queue = session.createQueue("queue:///" + qn).asInstanceOf[MQQueue]
      receiver = session.createReceiver(queue).asInstanceOf[MQQueueReceiver]
      consumer = session.createConsumer(queue)
      consumer.setMessageListener(new EventListener)
      connection.start()
      info("Connection Established to WSMQ :: " + connection.getPropertyNames)
    } catch {
      case t: Throwable =>
        restart("Cannot Establish Connection to WSMQ Trying to re-establish", t, 10000)
    }
  }

  override def onStop(): Unit = {
    closeResources()
  }

  private def closeResources(): Unit = {
    closeRes(session)
    closeRes(consumer)
    closeRes(receiver)
    try {
      if (connection != null) connection.stop()
    } catch {
      case t: Throwable => reportError("Closing Connection Failed ", t)
    }
  }

  private def closeRes(src: AutoCloseable): Unit = {
    try {
      info("Closing Resource :: " + src)
      if (src != null) src.close()
    } catch {
      case t: Throwable => reportError("Closing Resource Failed", t)
    }
  }

  private class EventListener extends MessageListener {
    override def onMessage(message: Message): Unit = {
      val persisted = Try(store(message.asInstanceOf[TextMessage].getText)) match {
        case Success(x) => true
        case _ => false
      }
      if (persisted) {
        try {
          message.acknowledge()
        } catch {
          case t: Throwable =>
            try {
              message.acknowledge()
            } catch {
              case t1: Throwable => reportError("Cannot Ack message with Id :: " + message.getJMSMessageID, t1)
            }
        }
      }
    }
  }

  override def close(): Unit = {
    closeResources()
    stop("Stopping WSMQ Receiver for " + appName)
  }

}
