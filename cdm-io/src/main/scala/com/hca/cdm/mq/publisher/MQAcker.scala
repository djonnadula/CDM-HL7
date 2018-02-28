package com.hca.cdm.mq.publisher

import java.util.concurrent.TimeUnit.SECONDS
import javax.jms.{ExceptionListener, JMSException, MessageProducer}
import com.hca.cdm._
import com.hca.cdm.exception.MqException
import com.hca.cdm.log.Logg
import com.hca.cdm.mq.MqConnector
import com.hca.cdm.utils.RetryHandler
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.concurrent.ExecutionContext.Implicits.{global => executionContext}
import scala.concurrent.duration.Duration
import scala.concurrent.{Future => async}
import scala.concurrent.Await._

case class TLMResponse(stage: String, active: Boolean, dest: String)

/**
  * Created by Devaraj Jonnadula on 3/6/2017.
  */
class MQAcker(app: String, jobDesc: String)(mqHosts: String, mqManager: String, mqChannel: String, queueMappingForResponse: String) extends Logg with MqConnector {

  self =>
  private lazy val batchSize = 5000
  private lazy val batchInterval = 0
  private lazy val restartInterval = 30000
  @volatile private var activeConnection: ConnectionMeta = _
  private var producer: Map[String, MessageProducer] = _
  private val ackQueuesActive = queueMappingForResponse.split(",", -1).map(dest => {
    val temp = dest.split(":", -1)
    temp(0) -> (temp(2).toBoolean, temp(1))
  }).toMap
  private val initialQueues = ackQueuesActive.map(active => {
    if (active._2._1) active._1 -> active._2._2
    else EMPTYSTR -> EMPTYSTR
  }).filter(x => x._1 != EMPTYSTR)

  initialise()


  @throws(classOf[MqException])
  def sendMessage(data: String, stage: String): Unit = synchronized {
    if (initialQueues.isDefinedAt(stage)) {
      if (!isConnectionBroken) self.activeConnection.sendMessage(data, producer(stage))
      else throw new MqException("Cannot Perform Operation. Connection Broken Try later")
    }
  }

  @throws(classOf[MqException])
  def sendMessages(msgs: Traversable[String], stage: String): Unit = synchronized {
    if (initialQueues.isDefinedAt(stage)) {
      if (!isConnectionBroken) msgs.foreach(msg => self.activeConnection.sendMessage(msg, producer(stage)))
      else throw new MqException("Cannot Perform Operation. Connection Broken Try later")
    }
  }

  @throws(classOf[MqException])
  def sendMessage(data: Array[Byte], stage: String): Unit = {
    sendMessage(new String(data, UTF8), stage)
  }

  @throws(classOf[MqException])
  private def initialise(): Unit = {
    require(valid(initialQueues), s"sCannot Send Acks to Queue Specified $initialQueues")
    handleConnection()
    activeConnection.addErrorListener(new ExceptionReporter)
    producer = initialQueues.map(queue => queue._1 -> activeConnection.createProducer(queue._2))
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
        initialQueues.foreach(que => activeConnection.createSession(que._2))
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


  override def toString: String = s"MQAcker(mqHosts=$mqHosts, mqManager=$mqManager, mqChannel=$mqChannel, ackQueue=$initialQueues)"


}

object MQAcker extends Logg {

  private val lock = new Object()
  private lazy val randomConn = new Random
  private var maxCon: Int = 1
  private var connections = new ArrayBuffer[MQAcker]

  @throws(classOf[MqException])
  def apply(app: String, jobDesc: String)(mqHosts: String, mqManager: String, mqChannel: String, queueMappingForResponse: String, numberOfIns: Int = 2): Unit = {
    maxCon = numberOfIns

    def createIfNotExist = new (() => MQAcker) {
      override def apply(): MQAcker = new MQAcker(app, jobDesc)(mqHosts, mqManager, mqChannel, queueMappingForResponse)
    }

    createConnection(numberOfIns, createIfNotExist)
  }

  private def createConnection(numberOfIns: Int, createIfNotExist: () => MQAcker): Unit = {
    lock.synchronized(
      if (connections isEmpty) {
        for (i <- 0 until numberOfIns) connections += createIfNotExist()
        info(s"Created Connection for $connections")
      })
  }

  def ackMessage(msg: String, stage: String): Unit = {
    tryAndLogThr(result(async {
      if (maxCon == 1) connections.head.sendMessage(msg, stage)
      else connections(randomConn.nextInt(connections.size)).sendMessage(msg, stage)
    }(executionContext), Duration(60, SECONDS)), EMPTYSTR, warn(_: Throwable), notify = false)

  }

  def ackMessages(msgs: Traversable[String], stage: String): Unit = {
    if (maxCon == 1) connections.head.sendMessages(msgs, stage)
    else connections(randomConn.nextInt(connections.size)).sendMessages(msgs, stage)
  }
}
