package com.cdm.mq

import javax.jms.{Message, MessageListener}

/**
  * Created by Devaraj Jonnadula on 12/27/2017.
  */
trait SourceListener extends MessageListener {

  def getSource: String

  def handleMessage(message: Message): Boolean

}
