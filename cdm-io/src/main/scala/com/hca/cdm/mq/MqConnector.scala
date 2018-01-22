package com.hca.cdm.mq

import javax.jms._
import com.ibm.mq.jms._
import com.hca.cdm.log.Logg
import com.hca.cdm.exception.{CdmException, MqException}
import com.hca.cdm._
import com.ibm.msg.client.wmq.common.CommonConstants._
import com.ibm.msg.client.wmq.compat.jms.internal.JMSC._
import scala.collection.concurrent.TrieMap
import scala.util.{Failure, Random, Success, Try}
import com.ibm.msg.client.jms.JmsConstants._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}


/**
  * Created by Devaraj Jonnadula on 12/6/2016.
  */
trait MqConnector extends Logg with AutoCloseable {

  private lazy val connections = new TrieMap[String, ConnectionMeta]()
  private lazy val consumers = new TrieMap[String, MessageConsumer]()
  private lazy val producers = new ListBuffer[MessageProducer]
  private lazy val sessions = new TrieMap[String, MQSession]()
  private lazy val randomSessions = new ArrayBuffer[MQSession]
  private lazy val randomSessionSelector = new Random()
  private val CHARSET = 437
  private val ENCODING = 546

  @throws[MqException]
  def createConnection(id: String, jobDesc: String, hosts: String, queueManager: String, channel: String, batchSize: Int, batchInterval: Int): ConnectionMeta = {
    connections.synchronized {
      if (connections.isDefinedAt(id)) return connections(id)
      var connection: Connection = null
      try {
        val factory = connectionFactory(id, jobDesc, hosts, queueManager, channel, batchSize, batchInterval)
        connection = factory createConnection()
        info(s"Connection Established to WSMQ with Id :: $id ")
        val meta = ConnectionMeta(factory, connection)
        connections += id -> meta
        meta
      }
      catch {
        case t: Throwable =>
          error("Cannot Establish Connection to WSMQ Trying to re-establish", t)
          closeResource(connection)
          throw new MqException(t)
      }
    }
  }

  private def connectionFactory(id: String, jobDesc: String, hosts: String, queueManager: String, channel: String, batchSize: Int, batchInterval: Int) = {
    val temp = new MQConnectionFactory
    if (valid(id) && id != EMPTYSTR) temp.setAppName(id)
    temp.setDescription(jobDesc)
    temp.setConnectionNameList(hosts)
    temp.setFailIfQuiesce(1)
    temp.setTransportType(MQJMS_TP_CLIENT_MQ_TCPIP)
    temp.setQueueManager(queueManager)
    temp.setChannel(channel)
    temp.setMsgBatchSize(batchSize)
    temp.setClientReconnectOptions(WMQ_CLIENT_RECONNECT)
    temp.setPollingInterval(batchInterval / 9)
    temp.setRescanInterval(batchInterval / 9)
    temp.setMaxBufferSize(120 * 1024 * 1024)
    info(s"Connection Factory Created to Hosts  :: ${temp.getConnectionNameList} with App Name $id")
    temp
  }


  case class ConnectionMeta(private val factory: MQConnectionFactory, private val connection: Connection) extends AutoCloseable {
    @throws[CdmException]
    def addEventListener(listener: SourceListener): Unit = {
      val consumer = createConsumer(listener.getSource)
      consumer setMessageListener listener
      info(s"Listener Added for Queue ${listener.getSource} with Consumer $consumer")
    }

    @throws[CdmException]
    def createConsumer(source: String, createNew: Boolean = false): MessageConsumer = {
      if ((consumers isDefinedAt source) && !createNew) consumers(source)
      else {
        closeResource(consumers.getOrElse(source,null))
        val consumer = tryAndThrow[MessageConsumer](createSession(source, createNew).createConsumer(new MQDestination(source)), error(_: Throwable))
        consumers += source -> consumer
        info(s"Consumer Created to consume from Queue $source $consumer")
        consumer
      }
    }

    @throws[CdmException]
    def addErrorListener(listener: ExceptionListener): Unit = {
      tryAndThrow(connection.setExceptionListener(listener), error(_: Throwable))
      info(s"Listener Added for Exceptions $listener")
    }

    @throws[CdmException]
    def resume(): Unit = {
      info(s"Starting Connection with with App Name ${factory.getAppName}")
      tryAndThrow(connection.start(), error(_: Throwable))
      info(s"Connection Started with with App Name ${factory.getAppName}")
    }

    @throws[CdmException]
    def pause(): Unit = {
      info(s"Stopping Connection with with App Name ${factory.getAppName}")
      tryAndThrow(connection.stop(), error(_: Throwable))
      info(s"Stopped Connection with with App Name ${factory.getAppName}")
    }

    @throws[MqException]
    def createProducer(destination: String): MessageProducer = {
      info(s"Creating Producer to Queue $destination")
      Try(createSession(destination).createProducer(createQueue(destination))) match {
        case Success(x) =>
          producers += x
          x
        case Failure(t) => throw new MqException(t)
      }
    }

    @throws[MqException]
    private def createQueue(destination: String): MQDestination = {
      info(s"Creating Queue $destination")
      Try(createSession(destination).createQueue(s"queue:///$destination").asInstanceOf[MQDestination]) match {
        case Success(x) =>
          x
        case Failure(t) => throw new MqException(t)
      }
    }

    @throws[CdmException]
    def createMessage(msg: String): Message = {
      val message = tryAndThrow[Message](randomSessions(randomSessionSelector.nextInt(randomSessions.size)).createTextMessage(msg), error(_: Throwable))
      debug(s"Message $msg created For destination ${message.getJMSDestination}")
      message.setIntProperty(JMS_IBM_CHARACTER_SET, CHARSET)
      message.setIntProperty(JMS_IBM_ENCODING, ENCODING)
      message
    }

    @throws[MqException]
    def sendMessage(msg: Message, producer: MessageProducer): Unit = {
      if (producer == null) {
        throw new MqException(s"Cannot Send message $msg to Queue ${producer.getDestination} . Register Destination first before performing this operation by calling createProducer()")
      } else {
        debug(s"Sending Message with Id ${msg.getJMSMessageID} to Queue ${producer.getDestination}")
        producer send(msg, new MQProducerRetryCallBack(producer))
      }
    }

    @throws[MqException]
    def sendMessage(msg: String, producer: MessageProducer): Unit = {
      sendMessage(createMessage(msg), producer)
    }

    @throws[MqException]
    def createAndSendMessage(msg: String, destination: String): Unit = {
      val prod = createProducer(destination)
      sendMessage(createMessage(msg), prod)
      closeResource(prod)
    }

    def createSession(source: String, createNew: Boolean = false): MQSession = {
      if ((sessions isDefinedAt source) && !createNew) sessions(source)
      else {
        closeResource(sessions.getOrElse(source, null))
        val session = connection.createSession(false, CLIENT_ACKNOWLEDGE).asInstanceOf[MQSession]
        info(s"Session Created for Source $source $session")
        sessions += source -> session
        randomSessions += session
        session
      }
    }

    override def close(): Unit = {
      info(s"Trying to Close resources for MQ with Id ${factory.getAppName} :: $connection")
      connections clear()
      consumers foreach (id => closeResource(id._2))
      producers foreach (closeResource(_))
      sessions foreach (_._2.close())
      consumers clear()
      producers clear()
      sessions clear()
      pause()
      closeResource(connection)
      info("All Resources Closed Completely ")
    }
  }


}






