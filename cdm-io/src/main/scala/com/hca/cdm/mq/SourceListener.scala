package com.hca.cdm.mq

import javax.jms.MessageListener

/**
  * Created by Devaraj Jonnadula on 12/27/2017.
  */
trait SourceListener extends MessageListener{

  def getSource : String

}
