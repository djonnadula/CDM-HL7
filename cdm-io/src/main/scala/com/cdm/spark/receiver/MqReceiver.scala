package com.cdm.spark.receiver

import java.util.concurrent.atomic.AtomicReference
import javax.jms._
import com.cdm.Models.MSGMeta
import com.cdm.io.IOConstants._
import com.cdm.log.Logg
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.storage.StorageLevel
import com.cdm.utils.RetryHandler
import com.cdm._
import com.cdm.exception.MqException
import com.cdm.mq.{MqConnector, SourceListener}
import scala.language.postfixOps
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}
import java.util.concurrent.TimeUnit._
import com.cdm.auth.LoginRenewer
import com.cdm.mq.publisher.MQAcker
import com.ibm.jms.JMSBytesMessage
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.{global => executionContext}
import scala.concurrent.duration.Duration
import scala.concurrent.{Future => async}
import scala.concurrent.Await._

case class MqData(source: String, data: String, msgMeta: MSGMeta)

/**
  * Created by Devaraj Jonnadula on 12/13/2016.
  */
class MqReceiver(nameNodes: String, id: Int, app: String, jobDesc: String, batchInterval: Int, batchSize: Int, sources: Set[String])
                (tlmAuditorMapping: Map[String, MSGMeta => String], metaFromRaw: String => MSGMeta, tlmAckStage: String)
  extends Receiver[MqData](storageLevel = StorageLevel.MEMORY_ONLY) with Logg with MqConnector {

  self =>
  private val mqHosts = lookUpProp("mq.hosts")
  private val mqManager = lookUpProp("mq.manager")
  private val mqChannel = lookUpProp("mq.channel")
  private val ackQueue = enabled(lookUpProp("mq.destination.queues"))
  private val mqHostsTlm = lookUpProp("mq.hosts.tlm")
  private val mqManagerTlm = lookUpProp("mq.manager.tlm")
  private val mqChannelTlm = lookUpProp("mq.channel.tlm")
  private val activeConnection = new AtomicReference[ConnectionMeta]
  private val restartTimeInterval = 30000
  private var consumerPool: ScheduledExecutorService = _
  private lazy val consumers = new mutable.HashMap[MessageConsumer, SourceListener]
  private val noMeta = MSGMeta(EMPTYSTR,currMillis.toString,EMPTYSTR,EMPTYSTR,"NOFAC")
  @volatile private var hookInit = false
  @volatile private var pauseConsuming = false

  def getCurrentConnection: ConnectionMeta = activeConnection.get()

  override def onStart(): Unit = synchronized {
    try {
      val con = handleConsumer()
      if (con == null) throw new MqException(s"Unable to Start MQ Connection with App Name $app")
      activeConnection set con
      init()
      LoginRenewer.scheduleRenewal(namesNodes = nameNodes, conf = None)
    } catch {
      case t: Throwable =>
        self.restart(s"Unable to Start Receiver with Id $app will make an attempt to Start Again", t, restartTimeInterval * 2)
    }
  }

  override def onStop(): Unit = synchronized {
    close()
  }


  private def init(): Unit = {
    sleep(batchInterval * 2)
    if (!hookInit) {
      sHook()
      hookInit = true
    }
    val con = activeConnection.get()
    if (con != null) {
      consumerPool = newDaemonCachedScheduler(s"WSMQ-Data-Fetcher-${self.id}", sources.size * 3)
      var tlmAckIO: String => Unit = null
      if (ackQueue.isDefined) {
        MQAcker(app, "appTLMRESPONSE")(mqHostsTlm, mqManagerTlm, mqChannelTlm, ackQueue.get)
        tlmAckIO = MQAcker.ackMessage(_: String, tlmAckStage)
        info(s"TLM IO Created $MQAcker for Queue $ackQueue")
      }
      sources.map(x => x.trim).toList foreach { queue =>
        val consumer = con createConsumer queue
        if (ackQueue isDefined) {
          consumers += consumer -> EventListener(queue, tlmAckIO)
        } else consumers += consumer -> EventListener(queue, null)
      }
      con addErrorListener new ExceptionReporter
      consumers foreach {
        case (consumer, handle) => consumerPool schedule(new DataConsumer(consumer, handle), batchInterval, TimeUnit.MILLISECONDS)
      }
      con resume()
    }
  }

  override def close(): Unit = {
    pauseConsuming = true
    info(s"Stopping $this ${self.id}")
    consumers foreach {
      con => closeResource(con._1)
    }
    consumers clear()
    if (valid(consumerPool)) {
      consumerPool awaitTermination(1, HOURS)
      consumerPool shutdown()
    }
    currentConnection foreach (closeResource(_))
    LoginRenewer.stop()
  }

  @throws(classOf[MqException])
  private def handleConsumer(): ConnectionMeta = {
    var con = activeConnection.get()
    info(s"Active Connection $con")
    if (con == null) {
      try {
        info(s"Starting MQ Consumer with App Name $app")
        con = createConnection(app, jobDesc, mqHosts, mqManager, mqChannel, batchSize, batchInterval)
      } catch {
        case ex: Exception => error("Consumer Connection Failed. Will Try To make connection based on Number of Re Tries Assigned", ex)
          var tryCount: Int = 1
          val retry = RetryHandler()
          while (retry.tryAgain()) {
            try {
              con = createConnection(app, jobDesc, mqHosts, mqManager, mqChannel, batchSize, batchInterval)
              info(s"createConnection Started After retries  $tryCount")
              return con
            } catch {
              case e: Exception => error(s"Consumer Connection Failed For Try :: $tryCount", e)
                tryCount += 1
                if (tryCount == defaultRetries) {
                  throw new MqException(s"Cannot Start MQ Connector After Retries $tryCount will wait ${restartTimeInterval * 2} ms and will try again ", e)
                }
            }
          }
      }
    }
    con
  }

  private case class EventListener(source: String, tlmAcknowledge: String => Unit) extends SourceListener {

    @throws[Exception]
    override def handleMessage(message: Message): Boolean = {
      val msg = message match {
        case txtMsg: TextMessage => txtMsg.getText
        case byteMsg: JMSBytesMessage =>
          val msgBuffer = new Array[Byte](byteMsg.getBodyLength.toInt)
          byteMsg readBytes msgBuffer
          new String(msgBuffer)
      }
      val data = msg.replaceAll("[\r\n]+", "\r\n")
      val meta = tryAndReturnDefaultValue0(metaFromRaw(data),noMeta)
      var persisted = false
      try {
        self.store(MqData(source, data, meta))
        persisted = true
      } catch {
        case t: Throwable =>
          error(s"Cannot Store message to Spark Storage ${message.getJMSMessageID}", t)
          self.pauseConsuming = true
          self.reportError(s"Cannot Write Message into Spark Memory, will replay Message with ID ${message.getJMSMessageID}, Stopping Receiver ${self.id}", t)
          self.stop(s"Cannot Write Message into Spark Memory, will replay Message with ID ${message.getJMSMessageID}, Stopping Receiver ${self.id}", t)
          throw t
      }
      if (ackQueue.isDefined) tryAndReturnThrow(tlmAcknowledge(tlmAuditorMapping(source)(meta)), s"TLM-Acknowledge for Source $source") match {
        case Right(t) =>
          self.reportError(s"TLM-Acknowledge for Source $source due to ${t.getMessage}", t)
        case _ =>
      }
      persisted
    }

    override def onMessage(message: Message): Unit = {
      handleMessage(message)
    }

    override def getSource: String = source
  }

  private def handleAcks(msg: Message): Unit = {
    try {
    //  tryAndThrow(msg.acknowledge(), error(_: Throwable))
    }
    catch {
      case t: Throwable =>
        error(s"Cannot Ack message with Id ::  ${msg.getJMSMessageID} will try with Retry Policy ", t)
        if (RetryHandler(msg.acknowledge)) info(s"Ack message with Id :: ${msg.getJMSMessageID} succeeded ")
        else error(s"Cannot Ack message with Id ::  ${msg.getJMSMessageID} After Retries")
    }
  }

  private def currentConnection: Option[ConnectionMeta] = if (valid(activeConnection.get())) Some(activeConnection.get()) else None

  private case class ExceptionReporter() extends ExceptionListener {
    override def onException(e: JMSException): Unit = {
      self.restart(s"$app Exception Raised in Connection ${activeConnection.get()} will make an attempt for Re-connection ", e, restartTimeInterval)
    }
  }

  private def sHook(): Unit = {
    registerHook(newThread(s"$id-$app-SHook", runnable(close())))
  }

  private class DataConsumer(private var consumer: MessageConsumer, sourceListener: SourceListener) extends Runnable {
    info(s"DataConsumer $this created for ${sourceListener.getSource} and consumer $consumer")
    private val noMessage: Message = null
    private var lastCommit: Long = currMillis
    private var fetCount: Long = 0
    private var storeReceived = true
    private var timeOut: Long = currMillis
    private var noMsgPoll: Long = 0
    private val batching: Boolean = false

    override def run(): Unit = {
      debug(s"${sourceListener.getSource} Receiver Stopped :: ${self.isStopped()} :: Requested Pause consuming ?? :: ${self.pauseConsuming} :: Store Received message :: $storeReceived")
      while (!self.isStopped()) {
        if (storeReceived && !self.pauseConsuming) {
          val msg = tryAndReturnDefaultValue(consumer.receiveNoWait, noMessage) match {
            case `noMessage` =>
              noMsgPoll = inc(noMsgPoll)
              if (noMsgPoll >= 500000) {
                tryAndReturnThrow(consumer.receive) match {
                  case Left(m) => m
                  case Right(t) =>
                    warn(s"No Message Received when polling for source ${sourceListener.getSource} since last commit $lastCommit ,total request made so far $noMsgPoll", t)
                    noMsgPoll = 0
                    noMessage
                }
              } else noMessage
            case message: Message => message
          }
          if (valid(msg)) {
            storeReceived = sourceListener handleMessage msg
            if (storeReceived) {
              if (batching) {
                fetCount = inc(fetCount)
                if (currMillis - lastCommit >= 30000 || fetCount >= 2000) {
                  lastCommit = currMillis
                  info(s"${sourceListener.getSource} Ack-ing for messages consumed so far count $fetCount at $lastCommit")
                  fetCount = 0
                  tryAndLogThr(result(async {
                    handleAcks(msg)
                  }(executionContext), Duration(60, SECONDS)), s"Acknowledger for Source ${sourceListener.getSource}", warn(_: Throwable), notify = false)
                }
              } else tryAndLogThr(result(async {
                handleAcks(msg)
              }(executionContext), Duration(60, SECONDS)), s"Acknowledger for Source ${sourceListener.getSource}", warn(_: Throwable), notify = false)
            }
          }
        }
        else {
          if (currMillis - timeOut >= 50000) {
            error(s"Consuming Data Stopped from WSMQ & source ${sourceListener.getSource} due to Data cannot be stored into Spark Storage")
            timeOut = currMillis
          }
        }
      }
    }

    private def mkNewConnectIfReq(msg: Message): Message = {
      if (msg == noMessage && currMillis - lastCommit >= 3000000) {
        tryAndLogErrorMes(
          asFunc(consumer = currentConnection.get.createConsumer(sourceListener.getSource, createNew = true)),
          error(_: Throwable), Some("Cannot create new Connection"))
        info(s"New Consumer $consumer Created for Source ${sourceListener.getSource} ")
        lastCommit = currMillis
        tryAndReturnDefaultValue0(consumer.receive(), msg)
      }
      else msg
    }

  }

}




