package com.hca.cdm.spark.receiver

import java.util.concurrent.atomic.AtomicReference
import javax.jms._
import com.hca.cdm.Models.MSGMeta
import com.hca.cdm.io.IOConstants._
import com.hca.cdm.log.Logg
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.storage.StorageLevel
import com.hca.cdm.utils.RetryHandler
import com.hca.cdm._
import com.hca.cdm.exception.MqException
import com.hca.cdm.mq.{MqConnector, SourceListener}
import scala.language.postfixOps
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit._
import com.hca.cdm.auth.LoginRenewer
import com.hca.cdm.mq.publisher.MQAcker
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
                (tlmAuditorMapping: Map[String, (MSGMeta) => String], metaFromRaw: (String) => MSGMeta, tlmAckStage: String)
  extends Receiver[MqData](storageLevel = StorageLevel.DISK_ONLY) with Logg with MqConnector {

  self =>
  private val mqHosts = lookUpProp("mq.hosts")
  private val mqManager = lookUpProp("mq.manager")
  private val mqChannel = lookUpProp("mq.channel")
  private val ackQueue = enabled(lookUpProp("mq.destination.queues"))
  private val activeConnection = new AtomicReference[ConnectionMeta]
  private val restartTimeInterval = 30000
  private var consumerPool: ThreadPoolExecutor = _
  private lazy val consumers = new mutable.HashMap[MessageConsumer, SourceListener]
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
    if (!hookInit) {
      sHook()
      hookInit = true
    }
    val con = activeConnection.get()
    if (con != null) {
      consumerPool = newDaemonCachedThreadPool(s"WSMQ-Data-Fetcher-${self.id}")
      var tlmAckIO: (String) => Unit = null
      if (ackQueue.isDefined) {
        MQAcker(app, "appTLMRESPONSE")(mqHosts, mqManager, mqChannel, ackQueue.get)
        tlmAckIO = MQAcker.ackMessage(_: String, tlmAckStage)
        info(s"TLM IO Created $MQAcker for Queue $ackQueue")
      }
      sources foreach { queue =>
        val consumer = con createConsumer queue
        if (ackQueue isDefined) {
          consumers += consumer -> EventListener(queue, tlmAckIO)
        } else consumers += consumer -> EventListener(queue, null)
      }
      con addErrorListener new ExceptionReporter
      consumers foreach {
        case (consumer, handle) => consumerPool submit new DataConsumer(consumer, handle)
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

  private case class EventListener(source: String, tlmAcknowledge: (String) => Unit) extends SourceListener {

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
      val meta = metaFromRaw(data)
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
      if (persisted) {
        tryAndLogThr(result(async {
          handleAcks(message, source, meta, tlmAcknowledge)
        }(executionContext), Duration(60, SECONDS)), s"Acknowledger for Source $source", warn(_: Throwable))
      }
      persisted
    }

    override def onMessage(message: Message): Unit = {
      handleMessage(message)
    }

    override def getSource: String = source
  }

  private def handleAcks(msg: Message, source: String, meta: MSGMeta, tlmAcknowledge: (String) => Unit): Unit = {
    try {
      tryAndThrow(msg.acknowledge(), error(_: Throwable))
      if (ackQueue.isDefined) tryAndLogThr(tlmAcknowledge(tlmAuditorMapping(source)(meta)), s"TLM-Acknowledge for Source $source", error(_: Throwable))
    }
    catch {
      case t: Throwable =>
        self.reportError(s"Cannot Ack message with Id ::  ${msg.getJMSMessageID} will try with Retry Policy ", t)
        var tryCount: Int = 1
        val retry = RetryHandler()
        while (retry.tryAgain()) {
          try {
            msg.acknowledge()
            info(s"Ack message with Id :: ${msg.getJMSMessageID} succeeded after tryCount $tryCount")
            if (ackQueue.isDefined) tryAndLogThr(tlmAcknowledge(tlmAuditorMapping(source)(meta)), s"TLM-Acknowledge for Source $source", error(_: Throwable))
            return
          } catch {
            case e: Exception => error(s"Cannot Ack message with Id ::  ${msg.getJMSMessageID} for Attempt Made So far " + tryCount, e)
              tryCount += 1
              if (tryCount == defaultRetries) {
                self.reportError(s"Cannot Ack message with Id ::  ${msg.getJMSMessageID} After Retries $tryCount", e)

              }
          }
        }
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

  private class DataConsumer(consumer: MessageConsumer, sourceListener: SourceListener) extends Runnable {
    var storeReceived = true
    var timeOut: Long = currMillis

    override def run(): Unit = {
      debug(s"${sourceListener.getSource} Receiver Stopped :: ${self.isStopped()} :: Requested Pause consuming ?? :: ${self.pauseConsuming} :: Store Received message :: $storeReceived")
      while (!self.isStopped()) {
        if (storeReceived && !self.pauseConsuming) {
          val msg = consumer.receiveNoWait
          if (valid(msg)) {
            storeReceived = sourceListener handleMessage msg
          }
        } else {
          if (currMillis - timeOut >= 50000) {
            error(s"Consuming Data Stopped from WSMQ & source ${sourceListener.getSource} due to Data cannot be stored into Spark Storage")
            timeOut = currMillis
          }
        }
      }
    }
  }

}




