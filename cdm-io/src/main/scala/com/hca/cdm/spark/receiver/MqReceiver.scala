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
import scala.util.{Failure, Success, Try}
import java.lang.System.{getenv => fromEnv}


case class MqData(source: String, data: String, msgMeta: MSGMeta)

/**
  * Created by Devaraj Jonnadula on 12/13/2016.
  */
class MqReceiver(id: Int, app: String, jobDesc: String, hosts: String, port: Int, queueManager: String, channel: String, batchInterval: Int, batchSize: Int, sources: Set[String])
                (tlmAuditorMapping: Map[String, (MSGMeta) => String], metaFromRaw: (String) => MSGMeta)
  extends Receiver[MqData](storageLevel = StorageLevel.MEMORY_ONLY_SER) with Logg with MqConnector {

  self =>
  private val ackQueue = {
    val tem = lookUpProp("mq.queueResponse")
    if (tem != EMPTYSTR) Some(tem)
    else None
  }
  private val activeConnection = new AtomicReference[ConnectionMeta]
  private val restartTimeInterval = 30000
  sHook()


  def getCurrentConnection: ConnectionMeta = activeConnection.get()

  override def onStart(): Unit = {
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

  override def onStop(): Unit = {
    close()
  }


  private def init(): Unit = {
    val con = activeConnection.get()
    if (con != null) {
      var prod: MessageProducer = null
      if (ackQueue isDefined) {
        prod = con createProducer ackQueue.get
        info(s"Created Initial Queue ${ackQueue.get} to Send TLM ACKS from Producer $prod")
      }
      sources.foreach(queue => {
        if (ackQueue isDefined) {
          con addEventListener EventListener(queue, con sendMessage(_: String, prod))
        } else con addEventListener EventListener(queue, null)
      })
      con addErrorListener new ExceptionReporter
      con resume()

    }
  }

  override def close(): Unit = {
    closeResource(activeConnection.get())
  }

  @throws(classOf[MqException])
  private def handleConsumer(): ConnectionMeta = {
    var con = activeConnection.get()
    info(s"Active Connection $con")
    if (con == null) {
      try {
        info(s"Starting MQ Consumer with App Name $app")
        con = createConnection(app, jobDesc, hosts, port, queueManager, channel, batchSize, batchInterval)
      } catch {
        case ex: Exception => error("Consumer Connection Failed. Will Try To make connection based on Number of Re Tries Assigned", ex)
          var tryCount: Int = 1
          val retry = RetryHandler()
          while (retry.tryAgain()) {
            try {
              con = createConnection(app, jobDesc, hosts, port, queueManager, channel, batchSize, batchInterval)
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
        case Success(x) =>
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
        case Failure(t) =>
          self.reportError(s"Cannot Write Message into Spark Memory, will replay Message with ID ${msg.getJMSMessageID}", t)
      }
    }

    override def getSource: String = source
  }

  private case class ExceptionReporter() extends ExceptionListener {
    override def onException(e: JMSException): Unit = {
      self.restart(s"$app Exception Raised in Connection ${activeConnection.get()} will make an attempt for Re-connection ", e, restartTimeInterval)
    }
  }

  private def sHook(): Unit = {
    registerHook(newThread(s"$id-$app-SHook", runnable({
      close()
      // self.stop(s"Stopping Receiver with Id $id and WSMQ Client Id $app Consuming Messages from WSMQ Queues ${sources.mkString(";")}")
    })))
  }

}




