package com.hca.cdm.mq.publisher

import javax.jms.{ExceptionListener, JMSException, MessageProducer}
import com.hca.cdm._
import com.hca.cdm.exception.MqException
import com.hca.cdm.log.Logg
import com.hca.cdm.mq.MqConnector
import com.hca.cdm.utils.RetryHandler
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * Created by Devaraj Jonnadula on 3/6/2017.
  */
class MQAcker(app: String, jobDesc: String, initialQueue: String)(mqHosts: String, mqManager: String, mqChannel: String) extends Logg with MqConnector {

  self =>
  private lazy val batchSize = 5000
  private lazy val batchInterval = 0
  private lazy val restartInterval = 30000
  @volatile private var activeConnection: ConnectionMeta = _
  private var producer: MessageProducer = _
  initialise()


  @throws(classOf[MqException])
  def sendMessage(data: String): Unit = synchronized {
    if (!isConnectionBroken) self.activeConnection.sendMessage(data, producer)
    else throw new MqException("Cannot Perform Operation. Connection Broken Try later")

  }

  @throws(classOf[MqException])
  def sendMessages(msgs: Traversable[String]): Unit = synchronized {
    if (!isConnectionBroken) msgs.foreach(msg => self.activeConnection.sendMessage(msg, producer))
    else throw new MqException("Cannot Perform Operation. Connection Broken Try later")
  }

  @throws(classOf[MqException])
  def sendMessage(data: Array[Byte]): Unit = {
    sendMessage(new String(data, UTF8))
  }

  @throws(classOf[MqException])
  private def initialise(): Unit = {
    require(valid(initialQueue) || initialQueue != EMPTYSTR, s"sCannot Send Acks to Queue Specified $initialQueue")
    handleConnection()
    activeConnection.addErrorListener(new ExceptionReporter)
    producer = activeConnection.createProducer(initialQueue)
    sHook()
  }


  private case class ExceptionReporter() extends ExceptionListener {
    override def onException(e: JMSException): Unit = {
      self.activeConnection.synchronized({
        error(s"$app Exception Raised in Connection $activeConnection will make an attempt for Re-connection in $restartInterval", e)
        activeConnection = null
        handleConnection()
      })
    }
  }

  private def isConnectionBroken: Boolean = {
    if (self.activeConnection == null) true
    else false
  }

  @throws(classOf[MqException])
  private def handleConnection(): ConnectionMeta = {
    info(s"Active Connection $activeConnection")
    if (activeConnection == null) {
      try {
        info(s"Starting MQ Producer with App Name $app")
        activeConnection = createConnection(app, jobDesc, mqHosts, mqManager, mqChannel, batchSize, batchInterval)
        activeConnection.createSession(initialQueue)
      } catch {
        case ex: Exception => error("Producer Connection Failed. Will Try To make connection based on Number of Re Tries Assigned", ex)
          val retry = RetryHandler()

          def connectionEst(): Unit = createConnection(app, jobDesc, mqHosts, mqManager, mqChannel, batchSize, batchInterval)

          if (!tryAndLogErrorMes(retry.retryOperation(connectionEst), error(_: Throwable))) throw new MqException(s"Cannot Start MQ Connector After Retries ${retry.triesMadeSoFar()}")
      }
    }
    activeConnection
  }


  override def close(): Unit = {
    closeResource(activeConnection)
  }

  private def sHook(): Unit = {
    registerHook(newThread(s"SHook-${this.getClass.getSimpleName}$app", runnable(close())))
  }

  override def toString = s"MQAcker(mqHosts=$mqHosts, mqManager=$mqManager, mqChannel=$mqChannel, ackQueue=$initialQueue)"

}

object MQAcker {

  private val lock = new Object()
  private lazy val randomConn = new Random
  private var connection = new ArrayBuffer[MQAcker]

  @throws(classOf[MqException])
  def apply(app: String, jobDesc: String, initialQueue: String)(mqHosts: String, mqManager: String, mqChannel: String, numberOfIns: Int = 1): Unit = {
    def createIfNotExist = new (() => MQAcker) {
      override def apply(): MQAcker = new MQAcker(app, jobDesc, initialQueue)(mqHosts, mqManager, mqChannel)
    }

    createConnection(numberOfIns, createIfNotExist)
  }

  private def createConnection(numberOfIns: Int, createIfNotExist: () => MQAcker): Unit = {
    lock.synchronized(
      if (connection isEmpty) {
        for (i <- 0 until numberOfIns) connection += createIfNotExist()
        info(s"Created Connection for $connection")
      })
  }

  def ackMessage(msg: String): Unit = {
    connection(randomConn.nextInt(connection.size)).sendMessage(msg)
  }

  def ackMessages(msgs: Traversable[String]): Unit = {
    connection(randomConn.nextInt(connection.size)).sendMessages(msgs)
  }
}
