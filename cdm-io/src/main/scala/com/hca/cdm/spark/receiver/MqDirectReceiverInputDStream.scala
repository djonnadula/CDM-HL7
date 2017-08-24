/*
package com.hca.cdm.spark.receiver

import java.util.concurrent.atomic.AtomicReference
import javax.jms.{ExceptionListener, JMSException, MessageProducer}

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
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.streaming.dstream.ReceiverInputDStream

/**
  * Created by Devaraj Jonnadula on 3/21/2017.
  */
class MqDirectReceiverInputDStream(id: Int, app: String, jobDesc: String, batchInterval: Int, batchSize: Int, sources: Set[String])
                                  (tlmAuditorMapping: Map[String, (MSGMeta) => String], metaFromRaw: (String) => MSGMeta)(strCtx: StreamingContext)
  extends ReceiverInputDStream[MqData](strCtx) with Logg with MqConnector{
  self =>
  private val mqHosts = lookUpProp("mq.hosts")
  private val mqManager = lookUpProp("mq.manager")
  private val mqChannel = lookUpProp("mq.channel")
  private val ackQueue = enabled(lookUpProp("mq.queueResponse"))
  private val activeConnection = new AtomicReference[ConnectionMeta]
  private val restartTimeInterval = 30000


  override def start(): Unit = {
    try {
      val con = handleConsumer()
      if (con == null) throw new MqException(s"Unable to Start MQ Connection with App Name $app")
      activeConnection set con
      init()
      sHook()
    } catch {
      case t: Throwable =>
        error(s"Unable to Start Receiver with Id $id ", t)
        throw new CdmException(s"Unable to Start Receiver with Id $id ", t)
    }
  }

  override def stop(): Unit = {
    close()
  }

  def getCurrentConnection: ConnectionMeta = activeConnection.get()

  override def name: String = s"WSMQ direct stream [$id]"

  override def getReceiver(): Receiver[MqData] = new MqReceiver(id, app, jobDesc, batchInterval, batchSize, sources)(tlmAuditorMapping, metaFromRaw)


  override def compute(validTime: Time): Option[RDD[MqData]] = {

    val sessionTime = validTime.milliseconds
    var sessionValid = currMillis
    while (sessionTime - sessionValid >0) {
      self.getCurrentConnection.createConsumer("").receive(sessionTime)
      sessionValid = currMillis
    }

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

   def close(): Unit = {
    closeResource(activeConnection.get())

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

  private def sHook(): Unit = {
    registerHook(newThread(s"$id-$app-SHook", runnable(close())))
  }

  private case class ExceptionReporter() extends ExceptionListener {
    override def onException(e: JMSException): Unit = {
      st
      self.restart(s"$app Exception Raised in Connection ${activeConnection.get()} will make an attempt for Re-connection ", e, restartTimeInterval)
    }
  }
}
*/
