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
import com.hca.cdm.exception.{CdmException, MqException}
import com.hca.cdm.mq.{MqConnector, SourceListener}
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}
import java.util.concurrent.{ThreadPoolExecutor, TimeUnit, TimeoutException}
import com.google.common.base.Throwables
import com.hca.cdm.mq.publisher.MQAcker
import scala.collection.mutable


case class MqData(source: String, data: String, msgMeta: MSGMeta)

/**
  * Created by Devaraj Jonnadula on 12/13/2016.
  */
class MqReceiver(id: Int, app: String, jobDesc: String, batchInterval: Int, batchSize: Int, sources: Set[String])
                (tlmAuditorMapping: Map[String, (MSGMeta) => String], metaFromRaw: (String) => MSGMeta)
  extends Receiver[MqData](storageLevel = StorageLevel.MEMORY_ONLY_2) with Logg with MqConnector {

  self =>
  private val mqHosts = lookUpProp("mq.hosts")
  private val mqManager = lookUpProp("mq.manager")
  private val mqChannel = lookUpProp("mq.channel")
  private val ackQueue = enabled(lookUpProp("mq.queueResponse"))
  private val activeConnection = new AtomicReference[ConnectionMeta]
  private val restartTimeInterval = 30000
  private var consumerPool: ThreadPoolExecutor = _
  private lazy val consumers = new mutable.HashMap[MessageConsumer, SourceListener]
  @volatile private var hookInit = false
  private var tlmAckIO: MQAcker = _
  @volatile private var storeCannotHandle = false

  def getCurrentConnection: ConnectionMeta = activeConnection.get()

  override def onStart(): Unit = synchronized {
    storeCannotHandle = false
    try {
      val con = handleConsumer()
      if (con == null) throw new MqException(s"Unable to Start MQ Connection with App Name $app")
      activeConnection set con
      init()
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
      if (ackQueue.isDefined) tlmAckIO = MQAcker(app, app, ackQueue.get)(mqHosts,mqManager,mqChannel)
      info(s"TLM IO Created $tlmAckIO for Queue $ackQueue")
      sources.foreach(queue => {
        val consumer = con createConsumer queue
        if (ackQueue isDefined) {
          consumers += consumer -> EventListener(queue, tlmAckIO.sendMessage(_: String))
        } else consumers += consumer -> EventListener(queue, null)
      })
      con addErrorListener new ExceptionReporter
      consumers foreach {
        consumer => consumerPool submit ConsumeData(consumer._1, consumer._2)
      }
      con resume()
    }
  }

  override def close(): Unit = {
    consumers foreach {
      con => closeResource(con._1)
    }
    consumers clear()
    if (valid(consumerPool)) {
      consumerPool awaitTermination(1, TimeUnit.HOURS)
      consumerPool shutdown()
    }
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
    override def onMessage(message: Message): Unit = {
      val msg = message.asInstanceOf[TextMessage]
      val data = msg.getText.replaceAll("[\r\n]+", "\r\n")
      val meta = metaFromRaw(data)
      Try(self.store(MqData(source, data, meta))) match {
        case Success(_) =>
          handleAcks(message, source, meta, tlmAcknowledge)
        case Failure(t) =>
          error(s"Cannot Write Message into Spark Memory, will Try with Retry Mechanism ${msg.getJMSMessageID}", t)
          storeCannotHandle = true
          val retry = RetryHandler()

          def job(): Unit = self.store(MqData(source, data, meta))

          if (tryAndLogErrorMes(retry.retryOperation(job), error(_: Throwable))) {
            handleAcks(message, source, meta, tlmAcknowledge)
            storeCannotHandle = false
          }
          else {
            self.reportError(s"Cannot Write Message into Spark Memory, will replay Message with ID ${msg.getJMSMessageID}", t)
            t match {
              case _: TimeoutException =>
                self.currentConnection.foreach(_.pause())
                self.stop(s"Cannot Write Message into Spark Memory Due to bad Response Times from HDFS, will replay Message with ID ${msg.getJMSMessageID}, Stopping Receiver ${self.id}", t)
                throw new CdmException(s"Cannot Write Message into Spark Memory Due to bad Response Times from HDFS, will replay Message with ID ${msg.getJMSMessageID}, Stopping Receiver ${self.id}", t)
              case _ =>
                self.currentConnection.foreach(_.pause())
                self.restart(s"Cannot Write Message into Spark Memory Due for Message with ID ${msg.getJMSMessageID}, Stopping Receiver ${self.id}", t)
            }
          }
      }
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

  private case class ConsumeData(consumer: MessageConsumer, sourceListener: SourceListener) extends Runnable {
    override def run(): Unit = {
      while (!self.isStopped()) {
        if (!storeCannotHandle) {
          try {
            consumer receiveNoWait match {
              case message: Message =>
                sourceListener onMessage message
              case _ =>
            }
          } catch {
            case e: Exception =>
              Throwables.getRootCause(e) match {
                case interrupted: InterruptedException =>
                  if (!self.isStopped()) {
                    error("Interrupted while receiving data from WSQM", interrupted)
                  }
                case exception: Exception =>
                  error("Error while receiving data from WSMQ", exception)
              }
          }
        }
      }
    }
  }

}




