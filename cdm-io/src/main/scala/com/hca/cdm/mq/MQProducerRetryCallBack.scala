package com.hca.cdm.mq

import javax.jms.{CompletionListener, Message, MessageProducer}
import com.hca.cdm.exception.MqException
import com.hca.cdm.io.IOConstants._
import com.hca.cdm.log.Logg
import com.hca.cdm.utils.RetryHandler
import com.hca.cdm._

/**
  * Created by Devaraj Jonnadula on 8/18/2016.
  *
  * Call back Impl that will retry to Send as per Config
  */
class MQProducerRetryCallBack(val handler: MessageProducer, val shouldRetry: Boolean = true) extends Logg with CompletionListener {

  private lazy val defaultRetries: Int = defaultRetries
  private lazy val waitBetweenTries = defaultWaitBetweenRetriesMS / 3
  private var retryHandler: RetryHandler = _


  override def onException(message: Message, exception: Exception): Unit = {
    if (!shouldRetry) {
      error(s"Failed to Send Record : ${message.getJMSMessageID} to Destination ${message.getJMSDestination}  Trying to Sending again .. ", exception)
      tryAndLogThr(handler.send(message), s"WSMQ Publisher for ${message.getJMSDestination}", error(_: Throwable))
    } else {
      if (retryHandler == null) this.retryHandler = RetryHandler(defaultRetries, waitBetweenTries)
      retrySend(exception, message)
    }
  }

  override def onCompletion(message: Message): Unit = {
    debug(s"Message Has been sent to Destination : ${message.getJMSDestination}  with ID  on  ${message.getJMSMessageID}")
  }

  private def retrySend(exception: Exception, data: Message): Unit = {
    while (retryHandler.tryAgain()) {
      error(s"Failed to Send Record : ${data.getJMSMessageID}  to Destination ${data.getJMSDestination}  Trying to send again with Retry Policy .. ", exception)
      handler.send(data, this)
    }
    giveUp(data)
  }

  private def giveUp(data: Message): Unit = {
    fatal(s"Failed to Send Record : $data to Destination ${data.getJMSDestination}  After tries $defaultRetries Giving UP ")
    throw new MqException(s"Sending Data to MQ Failed After tries $defaultRetries  Giving UP :: $data  with Message ID :: ${data.getJMSMessageID}")
  }


}

