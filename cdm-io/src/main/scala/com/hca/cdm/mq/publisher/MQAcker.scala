package com.hca.cdm.mq.publisher

import javax.jms.{ExceptionListener, JMSException, MessageProducer}
import com.hca.cdm._
import com.hca.cdm.exception.MqException
import com.hca.cdm.log.Logg
import com.hca.cdm.mq.MqConnector
import com.hca.cdm.utils.RetryHandler

/**
  * Created by Devaraj Jonnadula on 3/6/2017.
  */
private[cdm] class MQAcker(app: String, jobDesc: String, initialQueues: Array[String]) extends Logg with MqConnector {

  self =>
  private lazy val batchSize = 5000
  private lazy val batchInterval = 0
  private lazy val restartInterval = 30000
  private var activeConnection: ConnectionMeta = _
  private var producers: Map[String, MessageProducer] = _
  private val mqHosts = lookUpProp("mq.hosts")
  private val mqManager = lookUpProp("mq.manager")
  private val mqChannel = lookUpProp("mq.channel")
  private val ackQueue = {
    val tem = lookUpProp("mq.queueResponse")
    if (tem != EMPTYSTR) Some(tem)
    else None
  }
  initialise()

  @throws(classOf[MqException])
  def sendMessage(data: String, destination: String): Unit = {
    isConnectionBroken
    if (!(producers isDefinedAt destination)) producers + (destination -> activeConnection.createProducer(destination))
    self.activeConnection.sendMessage(data, producers(destination))
  }


  @throws(classOf[MqException])
  def sendMessage(data: Array[Byte], destination: String): Unit = {
    sendMessage(new String(data, UTF8), destination)
  }

  @throws(classOf[MqException])
  private def initialise(): Unit = {
    handleConnection()
    activeConnection.addErrorListener(new ExceptionReporter)
    producers = initialQueues.map(queue => queue -> activeConnection.createProducer(queue)).toMap
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
    if (self.activeConnection == null) throw new MqException("Cannot Perform Operation. Connection Broken Try later")
    else true
  }

  @throws(classOf[MqException])
  private def handleConnection(retry: Boolean = false): ConnectionMeta = {
    info(s"Active Connection $activeConnection")
    if (!retry && activeConnection == null) {
      try {
        info(s"Starting MQ Producer with App Name $app")
        activeConnection = createConnection(app, jobDesc, mqHosts, mqManager, mqChannel, batchSize, batchInterval)
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
    registerHook(newThread(s"SHook-${this.getClass.getSimpleName}", runnable(close())))
  }

  override def toString = s"MQAcker(mqHosts=$mqHosts, mqManager=$mqManager, mqChannel=$mqChannel, ackQueue=$ackQueue)"

}

object MQAcker {

  private val lock = new Object()
  private var connection: MQAcker = _

  @throws(classOf[MqException])
  def apply(app: String, jobDesc: String, initialQueues: Array[String]): MQAcker = {
    def createIfNotExist = new (() => MQAcker) {
      override def apply(): MQAcker = new MQAcker(app, jobDesc, initialQueues)
    }
    createConnection(createIfNotExist)
  }

  private def createConnection(createIfNotExist: () => MQAcker): MQAcker = {
    lock.synchronized(
      if (connection == null) {
        connection = createIfNotExist()
        info(s"Created Connection for $connection")
        connection
      } else {
        connection
      })
  }
}
